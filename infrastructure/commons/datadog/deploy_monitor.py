import os
from functools import lru_cache

import yaml
from datadog_api_client.v1 import ApiClient, Configuration, models
from datadog_api_client.v1.api import monitors_api
from pprint import pprint
import argparse
import copy

import monitor_settings


@lru_cache
def _env_name(target_env, nothing_ok=False):
    ret = {
        "Staging": "STG",
        "PreProduction": "PrePRD",
        "Production": "PRD",
    }.get(target_env, None)

    if ret is None and not nothing_ok:
        raise ValueError(f"{target_env} is invalid.")

    return ret


@lru_cache
def _error_notification_channel_name(target_env):
    if target_env.lower() == "Production".lower():
        return "@slack-negocia-10_sophia_ai_error_notification"
    else:
        return "@slack-negocia-10_sophia_ai_error_notification_dev"


@lru_cache
def _settings_class(aws_service_type, monitor_type):
    return {
        "Lambda": {
            "Error": monitor_settings.LambdaErrorMonitorSetting,
            "Warning": monitor_settings.LambdaWarningMonitorSetting,
            "MemoryUtilization": monitor_settings.LambdaMemoryUtilizationMonitorSetting,
        },
        "Batch": {
            "CPUUtilization": monitor_settings.BatchCPUUtilizationMonitorSetting,
            "MemoryUtilization": monitor_settings.BatchMemoryUtilizationMonitorSetting,
        },
    }[aws_service_type][monitor_type]


@lru_cache
def _tags(target_env, service_name, subsystem_name):
    return [
        "auto-deploy-monitor-managed",
        "auto-deploy-monitor-managed-service-name:{}{}".format(
            target_env.lower(),
            service_name.lower(),
        ),
        "auto-deploy-monitor-managed-subsysmtem-name:{}{}-{}".format(
            target_env.lower(),
            service_name.lower(),
            subsystem_name.lower(),
        ),
    ]


@lru_cache
def _datadog_api_configuration():
    return Configuration(api_key={
        "apiKeyAuth": os.environ["DATADOG_API_KEY"],
        "appKeyAuth": os.environ["DATADOG_APPLICATION_KEY"],
    })


def _delete_unmanaged_monitors(
    target_env, service_name, subsystem_name, managed_monitor_ids):

    with ApiClient(_datadog_api_configuration()) as api_client:
        api_instance = monitors_api.MonitorsApi(api_client)

        monitor_list = api_instance.list_monitors(
            monitor_tags=",".join(sorted(_tags(target_env, service_name, subsystem_name)))
        )

        for monitor in monitor_list:
            if monitor.id not in managed_monitor_ids:
                api_instance.delete_monitor(monitor.id)
                print(f"Deleted monitor. id: {monitor.id}, name: {monitor.name}")


def _call_deploy_api(name, setting):
    with ApiClient(_datadog_api_configuration()) as api_client:
        api_instance = monitors_api.MonitorsApi(api_client)

        monitor_list = api_instance.list_monitors(name=name)
        monitor_id = monitor_list[0].id if len(monitor_list) > 0 else None

        if monitor_id is None:
            print(f"create datadog monitor.")
            print(setting)
            api_response = api_instance.create_monitor(
                models.Monitor(**setting))
            monitor_id = api_response["id"]
        else:
            print(f"update datadog monitor. monitor_id={monitor_id}")
            print(setting)
            api_response = api_instance.update_monitor(
                    monitor_id, models.MonitorUpdateRequest(**setting))

    pprint(api_response)

    return monitor_id

def _monitor_name(target_env, service_name, subsystem_name, function_name, monitor_type):
    middle_name = function_name if function_name is not None else subsystem_name
    return f"[{_env_name(target_env)}]" + \
            f"[{service_name}][{middle_name}]{monitor_type}"


def _deploy_monitor(
    target_env,
    service_name,
    subsystem_name,
    function_name,
    lambda_function_name,
    aws_service_type,
    monitor_type,
    monitor_setting,
):
    monitor_name = _monitor_name(
        target_env, service_name, subsystem_name, function_name, monitor_type)

    error_notification_channel_name = _error_notification_channel_name(target_env)

    cfg = copy.deepcopy(monitor_setting)
    cfg["lambda_metric_namespace"] = f"{target_env}{service_name}.metric.lambda".lower()

    if lambda_function_name is None or "$" in lambda_function_name:
        cfg["lambda_metric_functionname"] = f"{target_env}{service_name}-{function_name}".lower()
        cfg["log_service_name"] = f"{target_env}{service_name}-{function_name}".lower()
    else:
        cfg["lambda_metric_functionname"] = f"{target_env}{service_name}-{subsystem_name}-{lambda_function_name}".lower()
        cfg["log_service_name"] = f"{target_env}{service_name}-{subsystem_name}-{lambda_function_name}".lower()

    cfg["batch_cluster_name_prefix"] = f"{target_env}{service_name}-{function_name}_batch".lower()

    setting_cls = _settings_class(aws_service_type, monitor_type)
    setting = setting_cls.get(monitor_name, error_notification_channel_name, cfg)

    setting["tags"] = _tags(target_env, service_name, subsystem_name)

    return _call_deploy_api(monitor_name, setting)


def deploy_monitor(target_env, datadog_cfg):
    if _env_name(target_env, nothing_ok=True) is None:
        print(f"{target_env} is not supported.")
        return

    service_name = datadog_cfg["service_name"]
    subsystem_name = datadog_cfg["subsystem_name"]
    deployed_monitor_ids = []
    for monitor_setting in datadog_cfg["monitor_settings"].values():
        for monitor_type in monitor_setting["monitor_types"]:
            if monitor_setting["aws_service_type"] == "Lambda":
                with open("serverless.yml") as file:
                    serverless = yaml.safe_load(file)
                function_names = serverless["functions"].keys()
                lambda_function_names = [x["name"].split("-")[-1]
                    for x in serverless["functions"].values()]

            else:
                function_names = [monitor_setting["function_name"]]
                lambda_function_names = [None]

            for function_name, lambda_function_name in zip(function_names, lambda_function_names):
                monitor_id = _deploy_monitor(
                    target_env,
                    service_name,
                    subsystem_name,
                    function_name,
                    lambda_function_name,
                    monitor_setting["aws_service_type"],
                    monitor_type,
                    monitor_setting,
                )

                deployed_monitor_ids.append(monitor_id)

    _delete_unmanaged_monitors(
        target_env,
        service_name,
        subsystem_name,
        deployed_monitor_ids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('target_env', help='e.g. Staging')
    args = parser.parse_args()

    with open('datadog_config.yml') as file:
        datadog_cfg = yaml.safe_load(file)

    deploy_monitor(args.target_env, datadog_cfg)

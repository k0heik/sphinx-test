from datadog_api_client.v1 import models


class BatchMemoryUtilizationMonitorSetting:
    @staticmethod
    def get(monitor_name, error_notification_channel_name, cfg):
        interval = "last_15m"
        threshold = 80

        message = '下記リンクからDatadogダッシュボードを確認してください。\n\n' \
             + 'https://app.datadoghq.com/dashboard/c9w-4it-8d9/ml\n\n' \
             + f'{error_notification_channel_name}'

        return {
            "message": message,
            "name": monitor_name,
            "options": models.MonitorOptions(
                notify_audit=False,
                locked=False,
                timeout_h=0,
                include_tags=True,
                notify_no_data=False,
                renotify_interval=0,
                evaluation_delay=600,
                thresholds=models.MonitorThresholds(
                    critical=float(threshold),
                    critical_recovery=float(threshold - 1),
                )
            ),
            "query": f'max({interval}):max:aws.ecs.memory_utilization'
                + f'{{clustername:{cfg["batch_cluster_name_prefix"]}*}} > {threshold}',
            "type": models.MonitorType("metric alert"),
        }

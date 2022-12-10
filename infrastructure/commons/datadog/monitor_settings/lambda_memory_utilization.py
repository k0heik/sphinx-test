from datadog_api_client.v1 import models


class LambdaMemoryUtilizationMonitorSetting:
    @staticmethod
    def get(monitor_name, error_notification_channel_name, cfg):
        interval = "last_15m"
        threshold = 80

        return {
            "message": error_notification_channel_name,
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
            "query": f'max({interval}):max:{cfg["lambda_metric_namespace"]}.MemoryUtilization'
                + f'{{functionname:{cfg["lambda_metric_functionname"]}}} > {threshold}',
            "type": models.MonitorType("metric alert"),
        }

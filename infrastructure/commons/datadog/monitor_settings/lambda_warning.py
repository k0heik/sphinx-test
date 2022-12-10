from datadog_api_client.v1 import models


class LambdaWarningMonitorSetting:
    @staticmethod
    def get(monitor_name, error_notification_channel_name, cfg):
        return {
            "message": error_notification_channel_name,
            "name": monitor_name,
            "options": models.MonitorOptions(
                enable_logs_sample=True,
                include_tags=True,
                locked=False,
                no_data_timeframe=10,
                notify_audit=False,
                notify_no_data=False,
                renotify_interval=0,
                timeout_h=0,
            ),
            "query": f'logs("service:{cfg["log_service_name"]} @http.status:WARNING")' +
                     '.index("*").rollup("count").last("5m") > 0',
            "type": models.MonitorType("log alert"),
        }

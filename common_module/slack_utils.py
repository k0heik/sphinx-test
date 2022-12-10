import slackweb


class SlackAttachmentColor:
    GOOD = "good"
    DANGER = "danger"
    WARNING = "warning"
    NONE = None


def send_slack_message(
    message,
    webhook_url,
    service_name="",
    title="",
    title_link="",
    color=SlackAttachmentColor.NONE,
    text=None
):
    slackweb.Slack(url=webhook_url).notify(
        text=text,
        link_names=1,
        attachments=[
            {
                "color": color,
                "author_name": service_name,
                "title": title,
                "title_link": title_link,
                "text": message,
            },
        ]
    )

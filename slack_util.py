import slack


class slack_util:

    def __init__(self, log, env):
        self.log = log
        self._client = slack.WebClient(token=env.get_value("SLACK_STATUS_OAUTH_TOKEN"))
        self._channel = "coindcx-bot-status-updates"

    def post_message(self, message):
        try:
            self._client.chat_postMessage(channel=self._channel, text=message)
            self.log.log_info('Posted message to Slack -> ' + message)
        except:
            self.log.log_exception('Error Occured')
            pass

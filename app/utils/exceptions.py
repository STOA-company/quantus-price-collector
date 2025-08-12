import requests
import os
from datetime import datetime

class BrokerConnectionError(Exception):
    """브로커 연결 오류"""
    pass

class CircuitBreakerError(Exception):
    """웹소켓 연결 오류"""
    
    def __init__(self, message="", broker_name="", failure_count=0):
        super().__init__(message)
        self.broker_name = broker_name
        self.failure_count = failure_count
        
        # 슬랙 알림 전송
        self.send_slack_notification()
    
    def send_slack_notification(self):
        """슬랙으로 에러 알림 전송"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🚨 서킷브레이커 에러 발생",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🚨 서킷브레이커 에러 발생"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{self.broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*실패횟수:*\n{self.failure_count}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*발생시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            }
                        ]
                    }
                ]
            }
            
            response = requests.post(slack_webhook_url, json=slack_message, timeout=5)
            if response.status_code != 200:
                print(f"슬랙 전송 실패: {response.status_code}")
                
        except Exception as e:
            print(f"슬랙 알림 전송 중 오류: {e}")

class SubscriptionError(Exception):
    """구독 오류"""
    pass


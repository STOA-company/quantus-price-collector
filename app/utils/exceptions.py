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

class BrokerInitializationError(Exception):
    """브로커 초기화 오류"""
    
    def __init__(self, message="", broker_name="", market_type="", error_details=""):
        super().__init__(message)
        self.broker_name = broker_name
        self.market_type = market_type
        self.error_details = error_details
        
        # 슬랙 알림 전송
        self.send_slack_notification()
    
    def send_slack_notification(self):
        """슬랙으로 브로커 초기화 에러 알림 전송"""
        
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🔴 브로커 초기화 실패",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔴 브로커 초기화 실패"
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
                                "text": f"*시장:*\n{self.market_type}"
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
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*에러 상세:*\n```{self.error_details}```"
                        }
                    }
                ]
            }
            
            response = requests.post(slack_webhook_url, json=slack_message, timeout=5)
            if response.status_code != 200:
                print(f"슬랙 전송 실패: {response.status_code}")
                
        except Exception as e:
            print(f"슬랙 알림 전송 중 오류: {e}")

class BrokerReconnectionError(Exception):
    """브로커 재연결 오류"""
    
    def __init__(self, message="", broker_name="", attempt_count=0, max_attempts=0, error_details=""):
        super().__init__(message)
        self.broker_name = broker_name
        self.attempt_count = attempt_count
        self.max_attempts = max_attempts
        self.error_details = error_details
        
        # 슬랙 알림 전송
        self.send_slack_notification()
    
    def send_slack_notification(self):
        """슬랙으로 브로커 재연결 에러 알림 전송"""
        
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🟡 브로커 재연결 실패",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🟡 브로커 재연결 실패"
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
                                "text": f"*시도 횟수:*\n{self.attempt_count}/{self.max_attempts}"
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
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*에러 상세:*\n```{self.error_details}```"
                        }
                    }
                ]
            }
            
            response = requests.post(slack_webhook_url, json=slack_message, timeout=5)
            if response.status_code != 200:
                print(f"슬랙 전송 실패: {response.status_code}")
                
        except Exception as e:
            print(f"슬랙 알림 전송 중 오류: {e}")

class BrokerDaemonStatusNotification:
    """브로커 데몬 상태 알림 (에러가 아닌 정보성 알림)"""
    
    @staticmethod
    def send_startup_notification(market_type: str, broker_count: int, symbol_count: int):
        """데몬 시작 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🚀 Broker Daemon 시작",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🚀 Broker Daemon 시작"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*시장:*\n{market_type}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커 수:*\n{broker_count}개"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*모니터링 종목:*\n{symbol_count}개"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*시작시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_shutdown_notification(market_type: str, uptime: str, total_messages: int):
        """데몬 종료 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🛑 Broker Daemon 종료",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🛑 Broker Daemon 종료"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*시장:*\n{market_type}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*가동시간:*\n{uptime}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*처리 메시지:*\n{total_messages:,}개"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*종료시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_broker_initialization_success(broker_name: str, market_type: str, session_count: int):
        """브로커 초기화 성공 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"✅ 브로커 초기화 성공",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "✅ 브로커 초기화 성공"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*시장:*\n{market_type}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*세션 수:*\n{session_count}개"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*완료시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_broker_reconnection_success(broker_name: str, attempt_count: int, wait_time: int):
        """브로커 재연결 성공 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🔄 브로커 재연결 성공",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔄 브로커 재연결 성공"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*시도 횟수:*\n{attempt_count}회"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*대기 시간:*\n{wait_time}초"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*연결시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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

    @staticmethod
    def send_circuit_breaker_open_notification(broker_name: str, failure_count: int, last_failure_time):
        """서킷브레이커 OPEN 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            last_failure_str = last_failure_time.isoformat() if last_failure_time else "알 수 없음"
            
            slack_message = {
                "text": f"🚨 서킷브레이커 OPEN",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🚨 서킷브레이커 OPEN"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*실패 횟수:*\n{failure_count}회"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*마지막 실패:*\n{last_failure_str}"
                            },
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
    
    @staticmethod
    def send_circuit_breaker_half_open_notification(broker_name: str):
        """서킷브레이커 HALF_OPEN 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🔄 서킷브레이커 HALF_OPEN",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔄 서킷브레이커 HALF_OPEN"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*상태:*\n복구 시도 중"
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
    
    @staticmethod
    def send_circuit_breaker_closed_notification(broker_name: str, recovery_type: str):
        """서킷브레이커 CLOSED 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"✅ 서킷브레이커 CLOSED",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "✅ 서킷브레이커 CLOSED"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*복구 타입:*\n{recovery_type}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*복구시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_websocket_connection_attempt_notification(broker_name: str, attempt_type: str):
        """웹소켓 연결 시도 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🔌 웹소켓 연결 시도",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔌 웹소켓 연결 시도"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*시도 타입:*\n{attempt_type}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*시도시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_websocket_connection_success_notification(broker_name: str):
        """웹소켓 연결 성공 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"✅ 웹소켓 연결 성공",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "✅ 웹소켓 연결 성공"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*연결시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_websocket_connection_failed_notification(broker_name: str, failure_reason: str):
        """웹소켓 연결 실패 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"❌ 웹소켓 연결 실패",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "❌ 웹소켓 연결 실패"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*실패 이유:*\n{failure_reason}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*실패시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
    @staticmethod
    def send_websocket_reconnection_attempt_notification(broker_name: str, attempt_type: str):
        """웹소켓 재연결 시도 알림"""
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🔄 웹소켓 재연결 시도",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔄 웹소켓 재연결 시도"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*브로커:*\n{broker_name}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*시도 타입:*\n{attempt_type}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*시도시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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

class RedisConnectionError(Exception):
    """Redis 연결 오류"""
    
    def __init__(self, message="", operation="", error_details=""):
        super().__init__(message)
        self.operation = operation
        self.error_details = error_details
        
        # 슬랙 알림 전송
        self.send_slack_notification()
    
    def send_slack_notification(self):
        """슬랙으로 Redis 연결 에러 알림 전송"""
        
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🔴 Redis 연결 에러 발생",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔴 Redis 연결 에러 발생"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*작업:*\n{self.operation}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*발생시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*에러 상세:*\n```{self.error_details}```"
                        }
                    }
                ]
            }
            
            response = requests.post(slack_webhook_url, json=slack_message, timeout=5)
            if response.status_code != 200:
                print(f"슬랙 전송 실패: {response.status_code}")
                
        except Exception as e:
            print(f"슬랙 알림 전송 중 오류: {e}")

class RedisOperationError(Exception):
    """Redis 작업 오류"""
    
    def __init__(self, message="", operation="", key="", error_details=""):
        super().__init__(message)
        self.operation = operation
        self.key = key
        self.error_details = error_details
        
        # 슬랙 알림 전송 (중요한 작업 실패 시에만)
        if self._should_send_notification():
            self.send_slack_notification()
    
    def _should_send_notification(self):
        """알림을 보낼지 결정 (중요한 작업만)"""
        critical_operations = ['publish_raw_data', 'set_broker_status']
        return self.operation in critical_operations
    
    def send_slack_notification(self):
        """슬랙으로 Redis 작업 에러 알림 전송"""
        
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            slack_message = {
                "text": f"🟡 Redis 작업 에러 발생",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🟡 Redis 작업 에러 발생"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*작업:*\n{self.operation}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*키:*\n{self.key}"
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
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*에러 상세:*\n```{self.error_details}```"
                        }
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

class ResubscriptionFailedError(Exception):
    """재구독 최종 실패 오류"""
    
    def __init__(self, message="", broker_name="", failed_symbols=None, max_retries=0, error_details=""):
        super().__init__(message)
        self.broker_name = broker_name
        self.failed_symbols = failed_symbols or []
        self.max_retries = max_retries
        self.error_details = error_details
        
        # 슬랙 알림 전송
        self.send_slack_notification()
    
    def send_slack_notification(self):
        """슬랙으로 재구독 최종 실패 에러 알림 전송"""
        
        try:
            slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not slack_webhook_url:
                return
            
            failed_symbols_str = ", ".join(self.failed_symbols) if self.failed_symbols else "없음"
            
            slack_message = {
                "text": f"🔴 재구독 최종 실패",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔴 재구독 최종 실패"
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
                                "text": f"*최대 시도 횟수:*\n{self.max_retries}회"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*실패한 종목 수:*\n{len(self.failed_symbols)}개"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*발생시간:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*실패한 종목:*\n```{failed_symbols_str}```"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*에러 상세:*\n```{self.error_details}```"
                        }
                    }
                ]
            }
            
            response = requests.post(slack_webhook_url, json=slack_message, timeout=5)
            if response.status_code != 200:
                print(f"슬랙 전송 실패: {response.status_code}")
                
        except Exception as e:
            print(f"슬랙 알림 전송 중 오류: {e}")
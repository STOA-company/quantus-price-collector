import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Any
from collections import deque

import requests

from app.brokers.base import BrokerOAuth


class TokenRateLimitError(Exception):
    """토큰 발급 횟수 제한 초과 예외"""
    def __init__(self, message, limit, time_window):
        self.limit = limit
        self.time_window = time_window
        super().__init__(message)


class TokenRequestError(Exception):
    """API 토큰 요청 과정에서 발생한 오류를 처리하기 위한 커스텀 예외"""
    def __init__(self, original_error, status_code=None, error_message=None, response_body=None):
        self.original_error = original_error
        self.status_code = status_code
        self.error_message = error_message
        self.response_body = response_body
        
        # 예외 정보를 객체 형태로 args에 저장
        error_info = {
            "original_error": original_error,
            "status_code": status_code,
            "error_message": error_message,
            "response_body": response_body
        }
        
        # args 튜플의 첫 번째 요소로 객체 전달
        super().__init__(error_info)


class DBFIOAuth(BrokerOAuth):
    """DBFI OAuth 클래스"""
    
    _instances = {}  # 토큰 인스턴스 저장 (앱키별)
    _lock = threading.Lock()
    BASE_URL = "https://openapi.dbsec.co.kr:8443"
    
    def __new__(cls, appkey: str, *args, **kwargs):
        # 앱키별로 인스턴스 생성/재사용 (세션 간에는 공유)
        with cls._lock:
            if appkey not in cls._instances:
                cls._instances[appkey] = super(DBFIOAuth, cls).__new__(cls)
            return cls._instances[appkey]

    def __init__(self, appkey: str, appsecretkey: str, headers: dict = {}):
        if hasattr(self, "appkey") and appkey == self.appkey \
            and hasattr(self, "appsecretkey") and appsecretkey == self.appsecretkey \
                and self.is_token_valid():
            # 동일한 API Key쌍 및 유효한 token에 대해서 재발급 생략
            return

        self.appkey = appkey
        self.appsecretkey = appsecretkey
        self.token = None
        self.expire_in = None
        self.token_type = None
        self.logger = logging.getLogger(__name__)
        self._initialized = True
        self.headers = headers

        # 토큰 발급 횟수 제한 (시간당 5회)
        self.token_request_limit = 5
        self.token_request_window = 3600  # 1시간 (초)
        self.token_request_history = deque()  # 발급 시각 저장

    def get_token(self) -> str:
        """액세스 토큰 발급"""
        if not self.is_token_valid():
            with DBFIOAuth._lock:
                if not self.is_token_valid():
                    self.request_token()
        return self.token

    def is_token_valid(self) -> bool:
        """토큰 유효성 확인"""
        if not self.token or not self.expire_in:
            return False
        return datetime.now() + timedelta(minutes=10) < self.expire_in

    def _check_token_request_limit(self) -> None:
        """토큰 발급 횟수 제한 체크"""
        now = datetime.now()

        # 시간 윈도우 밖의 오래된 기록 제거
        while self.token_request_history and \
              (now - self.token_request_history[0]).total_seconds() > self.token_request_window:
            self.token_request_history.popleft()

        # 제한 초과 체크
        if len(self.token_request_history) >= self.token_request_limit:
            oldest_request = self.token_request_history[0]
            wait_time = self.token_request_window - (now - oldest_request).total_seconds()

            error_msg = (
                f"토큰 발급 횟수 제한 초과: {self.token_request_limit}회/{self.token_request_window}초. "
                f"{wait_time:.0f}초 후 재시도 가능"
            )
            self.logger.error(f"🚨 {error_msg}")
            raise TokenRateLimitError(error_msg, self.token_request_limit, self.token_request_window)

    def request_token(self) -> None:
        """토큰 요청"""
        # 토큰 발급 횟수 제한 체크
        self._check_token_request_limit()

        headers = {"content-type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "appkey": self.appkey,
            "appsecretkey": self.appsecretkey,
            "scope": "oob",
        }
        try:
            self.logger.info("Requesting new access token from DB Securities API")
            response = requests.post(
                f"{self.BASE_URL}/oauth2/token", headers=headers, data=data
            )
            response.raise_for_status()
            token_data = response.json()

            self.token = token_data.get("access_token")
            expire_in = int(token_data.get("expires_in", 86400))
            self.expire_in = datetime.now() + timedelta(seconds=expire_in)
            self.token_type = token_data.get("token_type")

            # 발급 이력에 추가
            self.token_request_history.append(datetime.now())

            self.logger.info(
                f"New access token obtained. Valid until: {self.expire_in} "
                f"(발급 횟수: {len(self.token_request_history)}/{self.token_request_limit})"
            )
        except requests.exceptions.RequestException as e:
            status_code = None
            error_message = str(e)
            response_body = None
            
            # response 객체가 있는 경우 상태 코드와 응답 내용 추출
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                
                # 응답 본문 저장
                try:
                    response_body = e.response.json()
                    error_message = response_body.get('error_description', response_body.get('error', str(e)))
                except ValueError:
                    # JSON이 아닌 경우 텍스트 내용 사용
                    response_body = e.response.text
                    error_message = response_body
            
            self.logger.error(f"Failed to obtain access token: Status code: {status_code}, Error: {error_message}")
            raise TokenRequestError(e, status_code, error_message, response_body)

    def revoke_token(self) -> Dict[str, Any]:
        """토큰 해지"""
        if not self.token:
            self.logger.warning("No token to revoke")
            return {"code": 400, "message": "No token to revoke"}

        headers = {"content-type": "application/x-www-form-urlencoded"}
        data = {
            "appkey": self.appkey,
            "appsecretkey": self.appsecretkey,
            "token": self.token,
            "token_type_hint": "access_token",
        }
        try:
            self.logger.info("Revoking access token")
            response = requests.post(
                f"{self.BASE_URL}/oauth2/revoke", headers=headers, data=data
            )
            response.raise_for_status()
            result = response.json()
            if result.get("code") == 200:
                self.token = None
                self.expire_in = None
                self.token_type = None
                self.logger.info("Token successfully revoked")
            return result
        except requests.RequestException as e:
            self.logger.error(f"Failed to revoke token: {str(e)}")
            if hasattr(e, "response") and e.response:
                self.logger.error(f"Response: {e.response.text}")
            raise e

    def get_auth_header(self) -> Dict[str, str]:
        """인증 헤더 반환"""
        return {"authorization": f"{self.token_type} {self.get_token()}", **self.headers}

    def disconnect_session(self, account_no: str) -> Dict[str, Any]:
        """웹소켓 세션 초기화 - 모든 활성 세션 종료"""
        if not account_no:
            self.logger.warning("계좌번호가 없어 세션 초기화를 건너뜁니다")
            return {"code": 400, "message": "계좌번호 없음"}

        if not self.token:
            self.logger.warning("토큰이 없어 세션 초기화를 건너뜁니다")
            return {"code": 400, "message": "토큰 없음"}

        headers = {
            "Content-Type": "application/json;charset=utf-8",
            "authorization": f"Bearer {self.token}"
        }
        data = {"acntNo": account_no}

        try:
            self.logger.info(f"웹소켓 세션 초기화 시도 (계좌: {account_no})")
            response = requests.post(
                f"{self.BASE_URL}/api/v1/websocket/disconnectSession",
                headers=headers,
                json=data
            )
            response.raise_for_status()
            result = response.json()

            if result.get("result"):
                self.logger.info(f"세션 초기화 성공: {result.get('result')}")
            else:
                self.logger.warning(f"세션 초기화 응답: {result}")

            return result
        except requests.RequestException as e:
            self.logger.error(f"세션 초기화 실패: {str(e)}")
            if hasattr(e, "response") and e.response:
                self.logger.error(f"응답: {e.response.text}")
            raise e

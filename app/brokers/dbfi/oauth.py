import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Any

import requests

from app.brokers.base import BrokerOAuth


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
    
    _instance = None
    _lock = threading.Lock()

    BASE_URL = "https://openapi.dbsec.co.kr:8443"

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DBFIOAuth, cls).__new__(cls)
        return cls._instance

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

    def request_token(self) -> None:
        """토큰 요청"""
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
            self.logger.info(
                f"New access token obtained. Valid until: {self.expire_in}"
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

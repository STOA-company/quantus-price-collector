"""DB증권 토큰 발급 가드 — 실패도 «사용»으로 세는가.

2026-08-28 운영 실측: 토큰 발급이 403(IGW00201 호출 거래건수 초과)으로 막힌 상태에서
수집기가 10초마다 재시도를 계속해 한도를 스스로 붙잡고 있었다. 자체 가드(시간당 5회)가
있었는데도 풀린 이유는 두 가지다.

  1) 발급 이력을 **성공했을 때만** 기록했다 — 거절당하면 «사용량 0» 으로 보였다.
  2) 같은 앱키로 객체를 다시 만들면 이력을 초기화했다 — 그 초기화는 «유효한 토큰이 없을 때»,
     즉 정확히 실패 중일 때만 일어난다.

두 결함 다 «막아야 할 때 풀리는» 방향이다.
"""
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.brokers.dbfi.oauth import DBFIOAuth, TokenRateLimitError, TokenRequestError


class _RateLimitedResponse:
    """증권사가 한도 초과로 거절할 때의 응답."""

    status_code = 403

    def json(self):
        return {"error_code": "IGW00201", "error_description": "호출 거래건수를 초과하였습니다."}

    def raise_for_status(self):
        raise requests.exceptions.HTTPError("403 Client Error", response=self)


@pytest.fixture
def fresh_appkey(request):
    """앱키별 싱글턴이므로 테스트마다 새 키를 쓰고, 끝나면 인스턴스를 치운다."""
    key = f"test-appkey-{request.node.name}"
    yield key
    DBFIOAuth._instances.pop(key, None)


def test_rejected_requests_count_against_the_hourly_limit(fresh_appkey):
    """거절당한 발급도 한도를 소진해야 한다 — 아니면 가드가 실패 중에만 풀린다."""
    with patch("app.brokers.dbfi.oauth.requests.post", return_value=_RateLimitedResponse()) as post:
        auth = DBFIOAuth(fresh_appkey, "secret")
        for _ in range(auth.token_request_limit):
            with pytest.raises(TokenRequestError):
                auth.request_token()

        # 한도를 다 썼으므로 그 다음은 «묻지도 말아야» 한다.
        with pytest.raises(TokenRateLimitError):
            auth.request_token()

    assert post.call_count == auth.token_request_limit


def test_reconstructing_the_same_appkey_does_not_clear_the_history(fresh_appkey):
    """실패 중에 객체를 다시 만들어도 한도가 되살아나면 안 된다."""
    with patch("app.brokers.dbfi.oauth.requests.post", return_value=_RateLimitedResponse()) as post:
        auth = DBFIOAuth(fresh_appkey, "secret")
        for _ in range(auth.token_request_limit):
            with pytest.raises(TokenRequestError):
                auth.request_token()

        # 웹소켓·REST 폴백이 각각 생성한다 — 같은 앱키, 유효 토큰 없음.
        again = DBFIOAuth(fresh_appkey, "secret")
        with pytest.raises(TokenRateLimitError):
            again.request_token()

    assert post.call_count == auth.token_request_limit

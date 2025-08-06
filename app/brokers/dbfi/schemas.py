import json
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass
from datetime import datetime

from app.brokers.base import BrokerMessage, BrokerMessageBuilder, BrokerMessageParser, MarketType


class DBFIMarketType(Enum):
    """DBFI 시장 타입 열거형"""
    DOMESTIC = "S00"
    FOREIGN = "V60"


@dataclass
class DBFIHeader(BrokerMessage):
    """DBFI 메시지 헤더"""
    token: str
    tr_type: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'token': self.token,
            'tr_type': self.tr_type
        }

@dataclass
class DBFIBody(BrokerMessage):
    """DBFI 메시지 바디"""
    tr_cd: str
    tr_key: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'tr_cd': self.tr_cd,
            'tr_key': self.tr_key
        }

@dataclass
class DBFIMessage(BrokerMessage):
    """DBFI 메시지 구조"""
    header: DBFIHeader
    body: DBFIBody
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'header': self.header.to_dict(),
            'body': self.body.to_dict()
        }


@dataclass
class DBFISubscribeResponse(BrokerMessage):
    """구독 응답 데이터"""
    tr_cd: str
    rsp_cd: str
    rsp_msg: str
    tr_key: List[str]
    raw_data: Dict[str, Any]
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'subscribe_response',
            'tr_cd': self.tr_cd,
            'rsp_cd': self.rsp_cd,
            'rsp_msg': self.rsp_msg,
            'tr_key': self.tr_key,
            'raw_data': self.raw_data,
            'timestamp': self.timestamp
        }

@dataclass
class DBFIRealtimeData(BrokerMessage):
    """실시간 주식 데이터"""
    symbol: str
    date: str
    time: str
    current_price: int
    price_change: int
    price_change_rate: float
    open_price: int
    high_price: int
    low_price: int
    volume: int
    accumulated_volume: int
    ask_price: int
    bid_price: int
    ask_quantity: int
    bid_quantity: int
    price_color: str
    change_color: str
    rate_color: str
    raw_data: Dict[str, Any]
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'realtime_data',
            'symbol': self.symbol,
            'date': self.date,
            'time': self.time,
            'current_price': self.current_price,
            'price_change': self.price_change,
            'price_change_rate': self.price_change_rate,
            'open_price': self.open_price,
            'high_price': self.high_price,
            'low_price': self.low_price,
            'volume': self.volume,
            'accumulated_volume': self.accumulated_volume,
            'ask_price': self.ask_price,
            'bid_price': self.bid_price,
            'ask_quantity': self.ask_quantity,
            'bid_quantity': self.bid_quantity,
            'price_color': self.price_color,
            'change_color': self.change_color,
            'rate_color': self.rate_color,
            'raw_data': self.raw_data,
            'timestamp': self.timestamp
        }


class DBFIMessageBuilder(BrokerMessageBuilder):
    """DBFI 메시지 빌더"""
    
    def build_subscribe_message(self, symbol: str, market_type: MarketType, token: str) -> Dict[str, Any]:
        """구독 메시지 생성"""
        # MarketType을 DBFIMarketType으로 변환
        dbfi_market_type = DBFIMarketType.DOMESTIC if market_type == MarketType.DOMESTIC else DBFIMarketType.FOREIGN
        
        header = DBFIHeader(token=token, tr_type="1")
        body = DBFIBody(tr_cd=dbfi_market_type.value, tr_key=f"{symbol}")
        
        message = DBFIMessage(header=header, body=body)
        return message.to_dict()
    
    def build_unsubscribe_message(self, symbol: str, market_type: MarketType, token: str) -> Dict[str, Any]:
        """구독 해제 메시지 생성"""
        # MarketType을 DBFIMarketType으로 변환
        dbfi_market_type = DBFIMarketType.DOMESTIC if market_type == MarketType.DOMESTIC else DBFIMarketType.FOREIGN
        
        header = DBFIHeader(token=token, tr_type="2")
        body = DBFIBody(tr_cd=dbfi_market_type.value, tr_key=f"J {symbol}")
        
        message = DBFIMessage(header=header, body=body)
        return message.to_dict()


class DBFIMessageParser(BrokerMessageParser):
    """DBFI 메시지 파서"""
    
    def parse_message(self, raw_message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """메시지 파싱"""
        try:
            header = raw_message.get('header', {})
            body = raw_message.get('body', {})
            
            # body가 None인 경우 빈 딕셔너리로 처리
            if body is None:
                body = {}
            
            # 메시지 타입 확인
            tr_cd = header.get('tr_cd', '')
            tr_type = header.get('tr_type', '')
            #==================================해외주식 데이터 파싱 필요 =====================================================
            # 구독 응답 메시지 파싱
            if tr_cd == 'S00' and tr_type == '1':
                parsed_data = self._parse_subscribe_response(raw_message)
                parsed_message = parsed_data.to_dict()
            
            # 실시간 데이터 메시지 파싱
            elif tr_cd == 'S00' and 'ShrnIscd' in body:
                parsed_data = self._parse_realtime_data(raw_message)
                parsed_message = parsed_data.to_dict()
            
            # elif tr_cd == 'V60' and tr_type == '1':
            #     parsed_data = self._parse_subscribe_response(raw_message)
            #     parsed_message = parsed_data.to_dict()
            
            # # 실시간 데이터 메시지 파싱
            # elif tr_cd == 'V60' and 'ShrnIscd' in body:
            #     parsed_data = self._parse_realtime_data(raw_message)
            #     parsed_message = parsed_data.to_dict()
            #==================================해외주식 데이터 파싱 필요 =====================================================
            
            # 알 수 없는 메시지 타입
            else:
                parsed_message = {
                    'type': 'unknown',
                    'raw_data': raw_message,
                    'timestamp': datetime.now().isoformat()
                }
            
            if parsed_message:
                # 브로커 정보 추가
                parsed_message['broker'] = 'dbfi'
            
            return parsed_message
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"메시지 파싱 실패: {e}")
            return None
    
    def _parse_subscribe_response(self, raw_message: Dict[str, Any]) -> DBFISubscribeResponse:
        """구독 응답 메시지 파싱"""
        header = raw_message.get('header', {})
        body = raw_message.get('body', {})
        
        return DBFISubscribeResponse(
            tr_cd=header.get('tr_cd', ''),
            rsp_cd=header.get('rsp_cd', ''),
            rsp_msg=header.get('rsp_msg', ''),
            tr_key=body.get('tr_key', []),
            raw_data=raw_message,
            timestamp=datetime.now().isoformat()
        )
    
    def _parse_realtime_data(self, raw_message: Dict[str, Any]) -> DBFIRealtimeData:
        """실시간 데이터 메시지 파싱"""
        header = raw_message.get('header', {})
        body = raw_message.get('body', {})
        
        return DBFIRealtimeData(
            symbol=body.get('ShrnIscd', ''),
            date=body.get('BsopDate', ''),
            time=body.get('StckCntghour', ''),
            current_price=int(body.get('StckPrpr', 0)),
            price_change=int(body.get('PrdyVrss', 0)),
            price_change_rate=float(body.get('PrdyCtrt', 0)),
            open_price=int(body.get('StckOprc', 0)),
            high_price=int(body.get('StckHgpr', 0)),
            low_price=int(body.get('StckLwpr', 0)),
            volume=int(body.get('CntgVol', 0)),
            accumulated_volume=int(body.get('AcmlVol', 0)),
            ask_price=int(body.get('Askp1', 0)),
            bid_price=int(body.get('Bidp1', 0)),
            ask_quantity=int(body.get('AskpRsqn1', 0)),
            bid_quantity=int(body.get('BidpRsqn1', 0)),
            price_color=body.get('StckPrprclr', ''),
            change_color=body.get('PrdyVrssclr', ''),
            rate_color=body.get('PrdyCtrtclr', ''),
            raw_data=raw_message,
            timestamp=datetime.now().isoformat()
        )
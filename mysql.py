from sqlalchemy import inspect, Column, exists, Boolean, String, delete, Integer, DateTime, Text, select, Boolean, \
    Float, create_engine, update, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column, Session, relationship
import pymysql
import json
from sqlalchemy.sql import text
import pandas as pd
from datetime import datetime
import requests

#mysql orm with sqlalchemy


Base = declarative_base()
pymysql.install_as_MySQLdb()
engine = create_engine("mysql+mysqldb://user1:admin@localhost/kernel", echo=False)
connect = engine.connect()


class Candle(Base):
    __tablename__ = "candle"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[str] = mapped_column(String(255))
    symbol: Mapped[str] = mapped_column(String(255))
    time_frame: Mapped[str] = mapped_column(String(255))
    high_price: Mapped[float] = mapped_column(Float)
    open_price: Mapped[float] = mapped_column(Float)
    close_price: Mapped[float] = mapped_column(Float)
    low_price: Mapped[float] = mapped_column(Float)
    time: Mapped[float] = mapped_column(Float)
    volume: Mapped[str] = mapped_column(String(255))

    def df_in_database(self, data):
        data.to_sql('candle', engine, if_exists='append', index=False)

    def create(self, data: list):
        with Session(engine) as session:
            for i in data:
                date = i['date']
                symbol = i['symbol']
                time_frame = i['time_frame']
                high_price = i['high_price']
                open_price = i['open_price']
                close_price = i['close_price']
                low_price = i['low_price']
                time = i['time']
                volume = i['volume']

                candel = Candle(
                    date=date,
                    symbol=symbol,
                    time_frame=time_frame,
                    high_price=high_price,
                    open_price=open_price,
                    close_price=close_price,
                    low_price=low_price,
                    time=time,
                    volume=volume
                )
                session.add(candel)
                session.commit()

    def update(self, data):
        with Session(engine) as session:
            session.execute(update(Candle), data, )
            session.commit()

    def delete(self, time_frame, symbol=None):
        with Session(engine) as session:
            if symbol:
                session.execute(delete(Candle).where(Candle.symbol == symbol))
            else:
                session.execute(delete(Candle).where(Candle.time_frame == time_frame))

            session.commit()

    def read(self, symbol, time_frame, start_at, end_at, limit: int):
        with Session(engine) as session:
            result = session.query(Candle).filter(Candle.symbol == symbol, Candle.time_frame == time_frame,
                                                  Candle.time.in_([start_at, end_at])).limit(limit)
            return result

    def exist_symbol(self, symbol, time_frame):
        with Session(engine) as session:
            result = session.scalar(exists().where(Candle.symbol == symbol, Candle.time_frame == time_frame).select())
            return result


class Signal(Base):
    __tablename__ = "signals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    strategy_name: Mapped[str] = mapped_column(ForeignKey("strategies.strategy_name"))
    strategy: Mapped["Strategies"] = relationship(back_populates="signal")
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now())
    signal: Mapped[str] = mapped_column(String(255))

    def create(self, data: list):
        with Session(engine) as session:
            for i in data:
                name = i['strategy_name']
                strategy_signal = i['signal']
                signal = Signal(
                    strategy_name=name,
                    signal=strategy_signal,
                )
                session.add(signal)
                session.commit()

    def update(self, data):
        with Session(engine) as session:
            session.execute(update(Signal), data)
            session.commit()

    def delete(self, name: str, last: bool = True):
        with Session(engine) as session:
            if last:
                last_signal = session.query(Signal).where(Signal.strategy_name == name).order_by(
                    Signal.date.desc()).first()
                session.execute(delete(Signal).where(Signal.id == last_signal.id))
            else:
                session.execute(delete(Signal).where(Signal.strategy_name == name))

            session.commit()

    def read(self, filter=None):
        with Session(engine) as session:
            if filter:
                result = session.query(Signal).filter(Signal.strategy_name == filter)
            else:
                result = session.query(Strategies).all()
            return result


class Strategies(Base):
    __tablename__ = "strategies"
    strategy_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(255))
    time_frame: Mapped[str] = mapped_column(String(25))
    higher_time_frame: Mapped[str] = mapped_column(String(25))
    candel_count: Mapped[int] = mapped_column(Integer)
    candel_type: Mapped[str] = mapped_column(String(255))
    loader: Mapped[str] = mapped_column(String(255))
    mode: Mapped[str] = mapped_column(String(255))
    json_path: Mapped[str] = mapped_column(Text)
    check_existance: Mapped[str] = mapped_column(Boolean, default=False)
    signal: Mapped[str] = mapped_column(ForeignKey("signals.signal"))
    signal: Mapped[str] = relationship('Signal')

    def read_symbol_and_timeframe(self):
        with Session(engine) as session:
            symbols = session.query(Strategies.symbol).distinct()
            last = {}
            for i in symbols:
                result = session.query(Strategies.time_frame).filter(Strategies.symbol == i.symbol).distinct()
                ls = []
                for t in result:
                    ls.append(t.time_frame)
                last.update({i.symbol: ls})
        return last

    def read(self, filter=None):
        with Session(engine) as session:
            if filter:
                result = session.query(Strategies).filter(Strategies.strategy_name == filter)
            else:
                result = session.query(Strategies).all()
            return result

    def read_by_strategyname(self, strategy_name, *args, **kwargs):
        with Session(engine) as session:
            result = session.query(Strategies).filter(Strategies.strategy_name == strategy_name).all()
            session.close()
            if kwargs.get("return_objects"):
                return result

            return [vars(i) for i in result]

    def read_by_timeframe(self, timeframe, *args, **kwargs):
        with Session(engine) as session:
            result = session.query(Strategies).filter(Strategies.time_frame == timeframe).all()
            session.close()
            if kwargs.get("return_objects"):
                return result

            res_list = []
            for row in result:
                res_list.append(self.object_as_dict(row))
            return res_list

    def object_as_dict(self, obj):
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def update_by_strategyname(self, data_update_col: dict, strategy_name: str):
        with Session(engine) as session:
            u = update(Strategies)
            # data_update_col is a dict that you want to update
            u = u.values(data_update_col)
            u = u.where(Strategies.strategy_name == strategy_name)
            session.execute(u)
            session.commit()

    def delete(self, name: str):
        with Session(engine) as session:
            session.execute(delete(Strategies).where(Strategies.strategy_name == name))
            session.commit()

    def create(self, data: list):
        with Session(engine) as session:
            for i in data:
                name = i['strategy_name']
                symbol = i['symbol']
                time_frame = i['time_frame']
                higher_time_frame = i['higher_time_frame']
                candel_count = i['candel_count']
                candel_type = i['candel_type']
                loader = i['loader']
                mode = i['mode']
                path = i['json_path']
                strategy = Strategies(
                    strategy_name=name,
                    symbol=symbol,
                    time_frame=time_frame,
                    higher_time_frame=higher_time_frame,
                    candel_count=candel_count,
                    candel_type=candel_type,
                    loader=loader,
                    mode=mode,
                    json_path=path
                )
                session.add(strategy)
                session.commit()
            session.close()


Base.metadata.create_all(engine)
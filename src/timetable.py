from airflow.timetables.interval import CronDataIntervalTimetable

from src.schedules import schedule_enabled


class MarketTimetable(CronDataIntervalTimetable):
    def __init__(self, market, enabled=None):
        super().__init__("0 0 1 * *", timezone="UTC")
        self.market = market
        self.enabled = (
            schedule_enabled(market) if enabled is None else enabled
        )

    @property
    def summary(self):
        state = "on" if self.enabled else "off"
        return f"Monthly ({self.market}, schedule {state})"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval,
        restriction,
    ):
        if restriction.latest is None and not self.enabled:
            return None

        return super().next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )

    def serialize(self):
        return {
            "market": self.market,
            "enabled": self.enabled,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            market=data["market"],
            enabled=data["enabled"],
        )

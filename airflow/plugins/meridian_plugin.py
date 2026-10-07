from airflow.plugins_manager import AirflowPlugin

from src.timetable import MarketTimetable


class MeridianPlugin(AirflowPlugin):
    name = "meridian"
    timetables = [MarketTimetable]

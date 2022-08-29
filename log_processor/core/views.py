from collections import defaultdict

from rest_framework import status
from rest_framework.response import Response
from rest_framework.generics import GenericAPIView

from core.utility import pool_handler


class ProcessLog(GenericAPIView):
    @staticmethod
    def aggregate_data(data: list) -> dict:
        result = defaultdict(dict)
        for obj in data:
            for time, exc_dict in obj.items():
                for exc, count in exc_dict.items():
                    result[time][exc] = result[time].get(time, 0) + count
        return result

    @staticmethod
    def format_data(data: dict) -> list:
        def get_timestamp(time):
            hour, minute = time.split(":")
            hour, minute = int(hour), int(minute)
            next_hour, next_minute = hour, minute
            next_minute += 15
            if next_minute >= 60:
                next_minute %= 60
                next_hour += 1
                if next_hour > 23:
                    next_hour = 0
            hour, minute, next_minute, next_hour = (
                str(hour),
                str(minute),
                str(next_minute),
                str(next_hour),
            )
            if len(hour) < 2:
                hour = "0" + hour
            if len(minute) < 2:
                minute = "0" + minute
            if len(next_minute) < 2:
                next_minute = "0" + next_minute
            if len(next_hour) < 2:
                next_hour = "0" + next_hour
            return f"{hour}:{minute}-{next_hour}:{next_minute}"

        result = list()
        for time, exc_dict in data.items():
            logs = []
            exc_key = sorted(exc_dict.keys())
            for exc in exc_key:
                logs.append(dict(exception=exc, count=exc_dict[exc]))

            result.append(dict(timestamp=get_timestamp(time), logs=logs))
        return result

    def get(self, request, *args, **kwargs):
        try:
            log_files = request.data.get("logFiles")
            parallel_count = request.data.get("parallelFileProcessingCount", 0)
            if parallel_count == 0:
                return Response(
                    {
                        "status": "failure",
                        "reason": "Parallel File Processing count must be greater than zero!",
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            data = pool_handler(log_files, parallel_count)
            result_data = self.aggregate_data(data)
            result_data = self.format_data(result_data)
            return Response(dict(response=result_data), status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"status": "failure", "reason": str(e)},
                status=status.HTTP_400_BAD_REQUEST,
            )

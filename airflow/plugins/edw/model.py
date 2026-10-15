from dataclasses import dataclass


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[tuple[str, str], ...]
    keys: tuple[str, ...]
    required: tuple[str, ...]
    distribution: str
    rules: tuple[tuple[str, str], ...] = ()
    references: tuple[tuple[tuple[str, ...], str, tuple[str, ...]], ...] = ()
    other_unique: tuple[tuple[str, ...], ...] = ()

    @property
    def column_names(self):
        return tuple(name for name, _ in self.columns)


TABLES = (
    Table("aircrafts", (("aircraft_code", "text"), ("model", "text"), ("range", "integer")),
          ("aircraft_code",), ("aircraft_code", "model", "range"), "REPLICATED",
          (("invalid_range", "s.range <= 0"),)),
    Table("airports", (("airport_code", "text"), ("airport_name", "text"), ("city", "text"),
                       ("longitude", "double precision"), ("latitude", "double precision"), ("timezone", "text")),
          ("airport_code",), ("airport_code", "airport_name", "city", "longitude", "latitude", "timezone"), "REPLICATED",
          (("invalid_coordinates", "s.longitude NOT BETWEEN -180 AND 180 OR s.latitude NOT BETWEEN -90 AND 90"),)),
    Table("bookings", (("book_ref", "text"), ("book_date", "timestamptz"), ("total_amount", "numeric(18,2)")),
          ("book_ref",), ("book_ref", "book_date", "total_amount"), "BY (book_ref)",
          (("negative_amount", "s.total_amount < 0"),)),
    Table("seats", (("aircraft_code", "text"), ("seat_no", "text"), ("fare_conditions", "text")),
          ("aircraft_code", "seat_no"), ("aircraft_code", "seat_no", "fare_conditions"), "REPLICATED",
          (("invalid_fare", "s.fare_conditions NOT IN ('Economy','Comfort','Business')"),),
          ((("aircraft_code",), "aircrafts", ("aircraft_code",)),)),
    Table("flights", (("flight_id", "integer"), ("flight_no", "text"), ("scheduled_departure", "timestamptz"),
                      ("scheduled_arrival", "timestamptz"), ("departure_airport", "text"), ("arrival_airport", "text"),
                      ("status", "text"), ("aircraft_code", "text"), ("actual_departure", "timestamptz"),
                      ("actual_arrival", "timestamptz")),
          ("flight_id",), ("flight_id", "flight_no", "scheduled_departure", "scheduled_arrival", "departure_airport", "arrival_airport", "status", "aircraft_code"), "BY (flight_id)",
          (("invalid_schedule", "s.scheduled_arrival <= s.scheduled_departure"),
           ("invalid_actual_times", "s.actual_arrival IS NOT NULL AND (s.actual_departure IS NULL OR s.actual_arrival <= s.actual_departure)"),
           ("invalid_status", "s.status NOT IN ('On Time','Delayed','Departed','Arrived','Scheduled','Cancelled')")),
          ((("aircraft_code",), "aircrafts", ("aircraft_code",)),
           (("departure_airport",), "airports", ("airport_code",)),
           (("arrival_airport",), "airports", ("airport_code",))),
          (("flight_no", "scheduled_departure"),)),
    Table("tickets", (("ticket_no", "text"), ("book_ref", "text"), ("passenger_id", "text"),
                      ("passenger_name", "text"), ("contact_data", "jsonb")),
          ("ticket_no",), ("ticket_no", "book_ref", "passenger_id", "passenger_name"), "BY (ticket_no)",
          references=((("book_ref",), "bookings", ("book_ref",)),)),
    Table("ticket_flights", (("ticket_no", "text"), ("flight_id", "integer"), ("fare_conditions", "text"), ("amount", "numeric(18,2)")),
          ("ticket_no", "flight_id"), ("ticket_no", "flight_id", "fare_conditions", "amount"), "BY (ticket_no)",
          (("negative_amount", "s.amount < 0"), ("invalid_fare", "s.fare_conditions NOT IN ('Economy','Comfort','Business')")),
          ((("ticket_no",), "tickets", ("ticket_no",)), (("flight_id",), "flights", ("flight_id",)))),
    Table("boarding_passes", (("ticket_no", "text"), ("flight_id", "integer"), ("boarding_no", "integer"), ("seat_no", "text")),
          ("ticket_no", "flight_id"), ("ticket_no", "flight_id", "boarding_no", "seat_no"), "BY (ticket_no)",
          (("invalid_boarding_no", "s.boarding_no <= 0"),),
          ((("ticket_no", "flight_id"), "ticket_flights", ("ticket_no", "flight_id")),),
          (("flight_id", "boarding_no"), ("flight_id", "seat_no"))),
)

MART_KEYS = {
    "ticket_flight_details": ("ticket_no", "flight_id"),
    "flight_airport_daily": ("flight_date", "departure_airport", "arrival_airport"),
    "booking_sales_daily": ("booking_date",),
    "exchange_rates_daily": ("request_date", "currency_code"),
}


def create_flight_tables(cursor):
    for table in TABLES:
        columns = ", ".join(f'"{name}" {kind}' for name, kind in table.columns)
        storage = "" if table.distribution == "REPLICATED" else (
            "WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=1)"
        )
        for schema in ("staging", "core"):
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {schema}.{table.name} "
                f"({columns}, batch_id text NOT NULL, loaded_at timestamptz NOT NULL DEFAULT current_timestamp) "
                f"{storage} DISTRIBUTED {table.distribution}"
            )

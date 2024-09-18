from datetime import date, datetime, timedelta

NATIVE_DT_FORMAT = '%d.%m.%Y'
date_translation = {
    'Dnes': date.today().strftime(NATIVE_DT_FORMAT)
    , 'Včera': (date.today() - timedelta(days=1)).strftime(NATIVE_DT_FORMAT)
}


def parse_date(date_str: str) -> datetime.date:
    for key, value in date_translation.items():
        if key in date_str:
            return value

    try:
        return datetime.strptime(date_str, NATIVE_DT_FORMAT)
    except ValueError:
        return date.today()


if __name__ == '__main__':
    print(parse_date('01.01.2024'))

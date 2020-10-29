company_details = """CREATE TABLE public.company_details(
    act_symbol text COLLATE pg_catalog."default",
    company_name text COLLATE pg_catalog."default",
    exchange_code text COLLATE pg_catalog."default"
)"""

country_details = """CREATE TABLE public.country_details(
    country_code text COLLATE pg_catalog."default",
    country_name text COLLATE pg_catalog."default"
)"""

daily_stock = """CREATE TABLE public.daily_stock(
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume integer,
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    full_date integer
)"""

exchange_details = """CREATE TABLE public.exchange_details(
    country_code text COLLATE pg_catalog."default",
    exchange_code text COLLATE pg_catalog."default",
    exchange_name text COLLATE pg_catalog."default"
)"""

trade_day = """CREATE TABLE public.trade_day(
    full_date integer,
    day_of_week integer,
    week_of_year integer,
    month integer,
    year integer
)"""
from crawling_data import crawling_finance_data
from make_finance_db import make_finance_db
from make_value_factor import MakeValueDB, MakeUpsideDB


def main():

    crawling_finance_data()
    make_finance_db("q").create_db()
    make_finance_db("y").create_db()
    make_finance_db("ttm").create_db()

    MakeValueDB().run()
    MakeUpsideDB().run()

if __name__ == "__main__":
    main()
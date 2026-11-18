import random
import pandas as pd
from pymongo import MongoClient, ASCENDING

client = MongoClient("mongodb://localhost:27017")
db = client.shop
orders = db.orders


def run_lesson_examples():
    print("=== 1. LESSON EXAMPLES (CRUD & AGGREGATION) ===")
    orders.delete_many({})

    orders.insert_one({
        "_id": 1001,
        "customer": {"name": "Linh", "city": "Hanoi"},
        "status": "paid",
        "items": [
            {"sku": "BK-01", "title": "Python Basics", "qty": 2, "price": 12.5},
            {"sku": "BK-07", "title": "SQL in Practice", "qty": 1, "price": 18.0},
        ],
    })

    orders.insert_many([
        {
            "_id": 1002,
            "customer": {"name": "Nam", "city": "Da Nang"},
            "status": "shipped",
            "items": [{"sku": "BK-03", "title": "Data Viz", "qty": 1, "price": 22.0}],
        },
        {
            "_id": 1003,
            "customer": {"name": "Mai", "city": "Hanoi"},
            "status": "paid",
            "items": [{"sku": "BK-01", "title": "Python Basics", "qty": 1, "price": 12.5}],
            "coupon": "WELCOME10",
        },
        {
            "_id": 1004,
            "customer": {"name": "Hung", "city": "Hue"},
            "status": "paid",
            "items": [],
        },
    ])

    print("Paid orders:")
    for doc in orders.find({"status": "paid"}, {"customer.name": 1, "status": 1}):
        print(" ", doc)

    pipeline = [
        {"$match": {"status": {"$in": ["paid", "shipped"]}}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.sku",
                "units": {"$sum": "$items.qty"},
                "revenue": {"$sum": {"$multiply": ["$items.qty", "$items.price"]}},
            }
        },
        {"$sort": {"revenue": -1}},
        {"$limit": 5},
    ]
    df = pd.DataFrame(orders.aggregate(pipeline))
    print("\nTop SKUs by revenue:\n", df)


def exercise_1():
    print("\n=== EXERCISE 1: 5 DIFFERENT SHAPES & FIND BY SKU ===")
    orders.delete_many({})
    orders.insert_many([
        {
            "_id": 1,
            "customer": {"name": "An", "city": "Hanoi"},
            "status": "paid",
            "items": [{"sku": "BK-01", "qty": 1, "price": 12.5}],
        },
        {
            "_id": 2,
            "customer": {"name": "Binh", "city": "HCMC"},
            "status": "shipped",
            "items": [
                {"sku": "BK-02", "qty": 2, "price": 15.0},
                {"sku": "BK-01", "qty": 1, "price": 12.5},
            ],
            "coupon": "SALE20",
        },
        {
            "_id": 3,
            "customer": {"name": "Chi", "city": "Hanoi", "vip": True},
            "status": "paid",
            "items": [{"sku": "BK-03", "qty": 3, "price": 20.0}],
            "tags": ["express", "gift-wrap"],
        },
        {
            "_id": 4,
            "customer": {"name": "Dung", "city": "Da Nang"},
            "status": "pending",
            "items": [{"sku": "BK-01", "qty": 4, "price": 12.5}],
            "note": "Call before delivery",
        },
        {
            "_id": 5,
            "customer": {"name": "Em", "city": "HCMC"},
            "status": "paid",
            "items": [{"sku": "BK-04", "qty": 1, "price": 30.0}],
            "payment_method": "credit_card",
        },
    ])

    matches = list(orders.find({"items.sku": "BK-01"}, {"_id": 1, "customer.name": 1, "status": 1}))
    print("Orders containing sku 'BK-01':", matches)


def exercise_2():
    print("\n=== EXERCISE 2: REVENUE PER CITY ===")
    pipeline = [
        {"$match": {"status": {"$in": ["paid", "shipped"]}}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$customer.city",
                "total_revenue": {"$sum": {"$multiply": ["$items.qty", "$items.price"]}},
            }
        },
        {"$sort": {"total_revenue": -1}},
    ]
    df_city = pd.DataFrame(orders.aggregate(pipeline))
    print(df_city)


def exercise_3():
    print("\n=== EXERCISE 3: EXPLAIN() BEFORE & AFTER INDEX ON 10,000 DOCS ===")
    bench = db.orders_bench
    bench.drop()

    cities = ["Hanoi", "HCMC", "Da Nang", "Hue", "Hai Phong"]
    statuses = ["paid", "shipped", "pending", "cancelled"]
    bulk_docs = [
        {
            "_id": i,
            "customer": {"name": f"User_{i}", "city": random.choice(cities)},
            "status": random.choice(statuses),
            "total": round(random.uniform(10, 200), 2),
        }
        for i in range(10000)
    ]
    bench.insert_many(bulk_docs)

    query = {"customer.city": "Hanoi", "status": "paid"}

    before = bench.find(query).explain()["executionStats"]
    print(
        f"Before index -> stage: {before['executionStages']['stage']}, "
        f"docsExamined: {before['totalDocsExamined']}, "
        f"nReturned: {before['nReturned']}, "
        f"timeMs: {before['executionTimeMillis']}"
    )

    bench.create_index([("customer.city", ASCENDING), ("status", ASCENDING)])

    after = bench.find(query).explain()["executionStats"]
    print(
        f"After index  -> stage: {after['executionStages']['inputStage']['stage']}, "
        f"docsExamined: {after['totalDocsExamined']}, "
        f"nReturned: {after['nReturned']}, "
        f"timeMs: {after['executionTimeMillis']}"
    )


if __name__ == "__main__":
    run_lesson_examples()
    exercise_1()
    exercise_2()
    exercise_3()
    client.close()
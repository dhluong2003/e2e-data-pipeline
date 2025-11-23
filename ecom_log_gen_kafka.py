import argparse, json, random, sys, time, uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

# ============== Utilities ==============

DEVICES = ["web", "android", "ios"]
SOURCES = ["direct", "seo", "ads", "push", "email"]
CITIES = ["Hanoi", "HCMC", "DaNang", "HaiPhong", "CanTho"]
PAYMENTS = ["cod", "credit_card", "e_wallet", "bank_transfer"]
CATS = ["Fashion","Beauty","Electronics","Home","Sports"]

def iso(dt: datetime) -> str:
    # always ISO-8601 UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def rid(prefix=""):
    return f"{prefix}{uuid.uuid4().hex[:12]}"

def price_vnd(low=30000, high=3_000_000):
    return int(random.uniform(low, high)//1000*1000)

def clamp_positive(x: float) -> float:
    return x if x > 0 else 0.001

# ============== Kafka helpers ==============

def get_kafka_producer(bootstrap: str):
    """Return (produce_fn, flush_fn, kind) or (None,None,None). produce_fn(value_bytes, key_bytes)->None"""
    if not bootstrap:
        return None, None, None
    # Try confluent_kafka
    try:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": bootstrap,
                             "linger.ms": 50,
                             "batch.num.messages": 1000000000,
                             "queue.buffering.max.messages": 1000000000
                             })
        def produce(value_bytes: bytes, key_bytes: Optional[bytes], topic: str):
            def _cb(err, msg):
                if err:
                    print(f"[ERR] delivery failed: {err}", file=sys.stderr)
            producer.produce(topic, value=value_bytes, key=key_bytes, callback=_cb)
        def flush(timeout=10):
            producer.flush(timeout)
        return produce, flush, "confluent_kafka"
    except Exception:
        pass
    # Fallback kafka-python
    try:
        from kafka import KafkaProducer
        prod = KafkaProducer(bootstrap_servers=bootstrap,
                             value_serializer=lambda v: v,  # bytes already
                             key_serializer=lambda v: v,
                             linger_ms=50)
        def produce(value_bytes: bytes, key_bytes: Optional[bytes], topic: str):
            prod.send(topic, value=value_bytes, key=key_bytes)
        def flush(timeout=10):
            prod.flush(timeout)
        return produce, flush, "kafka_python"
    except Exception as e:
        print(f"[WARN] Kafka not available: {e}", file=sys.stderr)
        return None, None, None

# ============== Event building ==============

class SessionCtx:
    def __init__(self, user_pool:int, shop_pool:int, base_time:datetime):
        self.user_id = f"user_{random.randint(1, user_pool)}"
        self.shop_id = f"shop_{random.randint(1, shop_pool)}"
        self.session_id = uuid.uuid4().hex
        self.order_id: Optional[str] = None
        self.paid = False
        self.cart: List[Dict[str, Any]] = []
        self.t = base_time  # event time progressor

    def tick(self, min_ms=100, max_ms=2000):
        # advance time a bit to keep natural ordering within session
        self.t += timedelta(milliseconds=random.randint(min_ms, max_ms))

def base_envelope(ctx: SessionCtx, event_type: str) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "event_time": iso(ctx.t),
        "event_id": uuid.uuid4().hex,
        "trace_id": uuid.uuid4().hex,
        "user_id": ctx.user_id,
        "session_id": ctx.session_id,
        "shop_id": ctx.shop_id,
        "device": random.choice(DEVICES),
        "source": random.choice(SOURCES),
        "marketplace_id": "VN-ECOM",
        "geo": {"country": "VN", "city": random.choice(CITIES)},
    }

def build_event(ctx: SessionCtx, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    ev = base_envelope(ctx, event_type)
    ev.update(payload)
    return ev

def yield_session_events(user_pool:int, shop_pool:int, start_time: datetime) -> List[Dict[str, Any]]:
    ctx = SessionCtx(user_pool, shop_pool, base_time=start_time)
    seq: List[Dict[str, Any]] = []

    # session_start
    seq.append(build_event(ctx, "session_start", {}))
    ctx.tick()

    # 0..5 impressions
    for _ in range(random.randint(0,5)):
        pid = f"prod_{random.randint(1,5000)}"
        sku = f"sku_{random.randint(1,8000)}"
        seq.append(build_event(ctx, "product_impression", {"product_id": pid, "sku_id": sku, "category": random.choice(CATS)}))
        ctx.tick()

    # 1..5 product_view
    for _ in range(random.randint(1,5)):
        pid = f"prod_{random.randint(1,5000)}"
        sku = f"sku_{random.randint(1,8000)}"
        seq.append(build_event(ctx, "product_view", {"product_id": pid, "sku_id": sku, "category": random.choice(CATS)}))
        ctx.tick()

        # optionally search
        if random.random() < 0.3:
            q = random.choice(["áo thun", "giày running", "tai nghe bluetooth", "nồi chiên", "kem chống nắng"])
            seq.append(build_event(ctx, "search_query", {"query": q, "sort": random.choice(["relevance","price_asc","price_desc"])}))
            ctx.tick()
            if random.random() < 0.6:
                seq.append(build_event(ctx, "search_click", {"query": q, "product_id": pid}))
                ctx.tick()

        # wishlist?
        if random.random() < 0.25:
            seq.append(build_event(ctx, "wishlist_add", {"product_id": pid, "sku_id": sku}))
            ctx.tick()

        # add to cart?
        if random.random() < 0.35:
            qty = random.randint(1,3)
            unit_price = price_vnd()
            ctx.cart.append({"product_id": pid, "sku_id": sku, "qty": qty, "unit_price": unit_price})
            seq.append(build_event(ctx, "cart_add", {"product_id": pid, "sku_id": sku, "qty": qty, "unit_price": unit_price, "currency": "VND"}))
            ctx.tick()

    if len(ctx.cart) == 0:
        seq.append(build_event(ctx, "session_end", {}))
        return seq

    # checkout & order
    total = sum(i["qty"] * i["unit_price"] for i in ctx.cart)
    seq.append(build_event(ctx, "checkout_start", {"cart_items": len(ctx.cart), "total_amount": total, "currency": "VND"}))
    ctx.tick()
    ctx.order_id = f"ord_{random.randint(1,1000000)}"
    seq.append(build_event(ctx, "order_created", {"order_id": ctx.order_id, "total_amount": total, "currency": "VND"}))
    ctx.tick()

    # payment
    if random.random() < 0.85:
        ctx.paid = True
        pm = random.choice(PAYMENTS)
        seq.append(build_event(ctx, "order_paid", {"order_id": ctx.order_id, "payment_method": pm, "paid_amount": total, "currency": "VND"}))
        ctx.tick()
        # fulfillment path
        seq.append(build_event(ctx, "order_pack", {"order_id": ctx.order_id}))
        ctx.tick()
        seq.append(build_event(ctx, "order_ship", {"order_id": ctx.order_id}))
        ctx.tick()
        seq.append(build_event(ctx, "order_delivered", {"order_id": ctx.order_id}))
        ctx.tick()

        # post purchase
        r = random.random()
        if r < 0.15:
            seq.append(build_event(ctx, "review_submit", {"order_id": ctx.order_id, "rating": random.randint(4,5), "title": "Hàng tốt"}))
            ctx.tick()
        elif r < 0.20:
            seq.append(build_event(ctx, "order_return_request", {"order_id": ctx.order_id, "reason": "size_not_fit"}))
            ctx.tick()
            seq.append(build_event(ctx, "refund_issued", {"order_id": ctx.order_id, "amount": total}))
            ctx.tick()
    else:
        seq.append(build_event(ctx, "order_payment_failed", {"order_id": ctx.order_id, "reason": random.choice(["insufficient_funds","3ds_failed","network_error"])}))
        ctx.tick()

    seq.append(build_event(ctx, "session_end", {}))
    return seq

# ============== Driver ==============

def main():
    ap = argparse.ArgumentParser("E-commerce log generator → Kafka (JSON)")
    ap.add_argument("--start-time", type=str, default=None,
                    help="Thời điểm bắt đầu (ISO-8601, vd: 2025-08-20T08:00:00Z). Mặc định: now UTC.")
    ap.add_argument("--count", type=int, default=1000, help="Tổng số log cần sinh")
    ap.add_argument("--rps", type=float, default=0, help="Số log/giây (0 = nhanh nhất có thể)")
    ap.add_argument("--shops", type=int, default=200, help="Số lượng shop giả lập")
    ap.add_argument("--users", type=int, default=10000, help="Số lượng user giả lập")
    ap.add_argument("--kafka", type=str, default="", help="Kafka bootstrap servers (vd: localhost:9092)")
    ap.add_argument("--topic", type=str, default="ecom_events_json", help="Kafka topic")
    ap.add_argument("--stdout", action="store_true", help="In log ra stdout (JSON Lines)")
    ap.add_argument("--seed", type=int, default=None, help="Random seed (tùy chọn)")
    args = ap.parse_args()

    # seed
    if args.seed is not None:
        random.seed(args.seed)

    # parse start time
    if args.start_time:
        try:
            st = args.start_time.replace("Z","+00:00")
            base_time = datetime.fromisoformat(st)
            if base_time.tzinfo is None:
                base_time = base_time.replace(tzinfo=timezone.utc)
            else:
                base_time = base_time.astimezone(timezone.utc)
        except Exception as e:
            print(f"[WARN] Không parse được --start-time, dùng now UTC. Lỗi: {e}", file=sys.stderr)
            base_time = datetime.now(timezone.utc)
    else:
        base_time = datetime.now(timezone.utc)

    # kafka
    produce_fn, flush_fn, kind = get_kafka_producer(args.kafka) if args.kafka else (None, None, None)
    if produce_fn:
        print(f"[INFO] Kafka producer = {kind}", file=sys.stderr)
    elif args.kafka:
        print("[WARN] Kafka không khả dụng, sẽ chỉ in stdout (nếu --stdout).", file=sys.stderr)

    target_events = max(1, args.count)
    sent = 0
    interval = (1.0 / args.rps) if args.rps and args.rps > 0 else 0.0
    session_start_time = base_time

    try:
        while sent < target_events:
            # Jitter mỗi session bắt đầu
            session_time = session_start_time + timedelta(milliseconds=random.randint(0, 2000))
            seq = yield_session_events(args.users, args.shops, session_time)

            for ev in seq:
                if sent >= target_events:
                    break

                ev["dt"] = ev["event_time"][:10]

                # serialize
                payload = json.dumps(ev, ensure_ascii=False).encode("utf-8")
                key = ev["user_id"].encode("utf-8")

                # stdout
                if args.stdout or not produce_fn:
                    print(payload.decode("utf-8"))

                # kafka
                if produce_fn:
                    produce_fn(payload, key, args.topic)

                sent += 1

                # pace
                if interval > 0:
                    time.sleep(interval)

            session_start_time += timedelta(seconds=random.uniform(0.2, 1.5))

        if flush_fn:
            flush_fn(10)
        print(f"[DONE] Generated {sent} events.", file=sys.stderr)

    except KeyboardInterrupt:
        if flush_fn:
            flush_fn(5)
        print(f"\n[STOP] Interrupted at {sent} events.", file=sys.stderr)

if __name__ == "__main__":
    main()
import asyncio
import uuid


def payload(text="Hi", user_id="u1", conversation_id="c1", client_message_id=None):
    body = {"user_id": user_id, "conversation_id": conversation_id, "text": text}
    if client_message_id is not None:
        body["client_message_id"] = client_message_id
    return body


async def test_post_message_is_stored_as_received(client):
    res = await client.post("/messages", json=payload(client_message_id="m-1"))

    assert res.status_code == 201
    body = res.json()
    assert body["state"] == "RECEIVED"
    assert body["seq"] == 1
    assert body["client_message_id"] == "m-1"
    assert body["assistant_text"] is None
    uuid.UUID(body["message_id"])


async def test_seq_follows_send_order_per_conversation(client):
    seqs = []
    for text in ("first", "second", "third"):
        res = await client.post("/messages", json=payload(text))
        seqs.append(res.json()["seq"])
    other = await client.post("/messages", json=payload("elsewhere", conversation_id="c2"))

    assert seqs == [1, 2, 3]
    assert other.json()["seq"] == 1


async def test_concurrent_sends_get_unique_contiguous_seqs(client):
    responses = await asyncio.gather(
        *(client.post("/messages", json=payload(f"msg {i}")) for i in range(20))
    )

    assert all(r.status_code == 201 for r in responses)
    assert sorted(r.json()["seq"] for r in responses) == list(range(1, 21))


async def test_resubmitting_client_message_id_returns_original(client):
    first = await client.post("/messages", json=payload("hello", client_message_id="dup-1"))
    again = await client.post("/messages", json=payload("hello again", client_message_id="dup-1"))

    assert again.status_code == 201
    assert again.json()["message_id"] == first.json()["message_id"]
    assert again.json()["text"] == "hello"
    convo = (await client.get("/conversations/u1")).json()
    assert len(convo["conversations"][0]["messages"]) == 1


async def test_server_generates_client_message_id_when_missing(client):
    a = (await client.post("/messages", json=payload("a"))).json()
    b = (await client.post("/messages", json=payload("a"))).json()

    assert a["client_message_id"] != b["client_message_id"]
    assert [a["seq"], b["seq"]] == [1, 2]


async def test_conversations_are_grouped_and_ordered_by_seq(client):
    for conv, text in [("c2", "x1"), ("c1", "y1"), ("c2", "x2"), ("c1", "y2")]:
        await client.post("/messages", json=payload(text, conversation_id=conv))
    await client.post("/messages", json=payload("not mine", user_id="u2"))

    body = (await client.get("/conversations/u1")).json()

    assert body["user_id"] == "u1"
    assert [c["conversation_id"] for c in body["conversations"]] == ["c1", "c2"]
    assert [m["text"] for m in body["conversations"][1]["messages"]] == ["x1", "x2"]
    assert [m["seq"] for m in body["conversations"][0]["messages"]] == [1, 2]


async def test_conversations_filter_by_conversation_id(client):
    await client.post("/messages", json=payload("a", conversation_id="c1"))
    await client.post("/messages", json=payload("b", conversation_id="c2"))

    body = (await client.get("/conversations/u1", params={"conversation_id": "c2"})).json()

    assert [c["conversation_id"] for c in body["conversations"]] == ["c2"]


async def test_unknown_user_has_no_conversations(client):
    body = (await client.get("/conversations/nobody")).json()
    assert body == {"user_id": "nobody", "conversations": []}


async def test_message_status(client):
    created = (await client.post("/messages", json=payload())).json()

    res = await client.get(f"/messages/{created['message_id']}/status")

    assert res.status_code == 200
    body = res.json()
    assert body["state"] == "RECEIVED"
    assert body["attempt_count"] == 0
    assert body["completed_at"] is None


async def test_message_status_unknown_id_is_404(client):
    res = await client.get(f"/messages/{uuid.uuid4()}/status")
    assert res.status_code == 404


async def test_message_status_rejects_malformed_id(client):
    res = await client.get("/messages/not-a-uuid/status")
    assert res.status_code == 422


async def test_empty_text_is_rejected(client):
    res = await client.post("/messages", json=payload(""))
    assert res.status_code == 422


async def test_same_key_from_another_user_is_a_separate_message(client):
    mine = await client.post("/messages", json=payload("mine", user_id="u1", client_message_id="k"))
    theirs = await client.post("/messages", json=payload("theirs", user_id="u2", client_message_id="k"))

    assert theirs.status_code == 201
    assert theirs.json()["message_id"] != mine.json()["message_id"]
    assert theirs.json()["user_id"] == "u2"
    assert theirs.json()["text"] == "theirs"
    u2 = (await client.get("/conversations/u2")).json()
    assert [m["text"] for m in u2["conversations"][0]["messages"]] == ["theirs"]


async def test_concurrent_duplicate_submissions_create_one_message(client):
    responses = await asyncio.gather(
        *(client.post("/messages", json=payload("retry storm", client_message_id="same")) for _ in range(10))
    )

    assert all(r.status_code == 201 for r in responses)
    assert len({r.json()["message_id"] for r in responses}) == 1
    follow_up = await client.post("/messages", json=payload("next"))
    assert follow_up.json()["seq"] == 2

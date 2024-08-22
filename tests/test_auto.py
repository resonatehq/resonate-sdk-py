from __future__ import annotations

import sys

import httpx
import pytest
from resonate.storage import RemotePromiseStore


def test_case_0_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id0",
        ikey=None,
        strict=True,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id0"
    assert promise_record.idempotency_key_for_create is None
    assert promise_record.idempotency_key_for_complete is None


def test_case_1_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id1",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id1"
    assert promise_record.idempotency_key_for_create is None
    assert promise_record.idempotency_key_for_complete is None


def test_case_2_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id2",
        ikey="ikc",
        strict=True,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id2"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_3_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id3",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id3"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_16_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id16",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id16",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_17_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id17",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id17",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_18_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id18",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id18",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_19_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id19",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id19",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_32_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id32",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id32",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_33_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id33",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id33",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_34_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id34",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    promise_record = store.create(
        promise_id="id34",
        ikey="ikc",
        strict=True,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id34"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_35_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id35",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    promise_record = store.create(
        promise_id="id35",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id35"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_36_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id36",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id36",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_37_transition_from_pending_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id37",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id37",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_50_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id50",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id50",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_51_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id51",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id51",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_52_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id52",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id52",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_53_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id53",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id53",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_66_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id66",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id66",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_67_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id67",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id67",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_68_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id68",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id68",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_69_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id69",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id69",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_88_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id88",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id88",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_89_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id89",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id89",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_90_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id90",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id90",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_91_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id91",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    promise_record = store.create(
        promise_id="id91",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "RESOLVED"
    assert promise_record.promise_id == "id91"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_92_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id92",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id92",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_93_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id93",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id93",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_106_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id106",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id106",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_107_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id107",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id107",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_108_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id108",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id108",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_109_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id109",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    promise_record = store.create(
        promise_id="id109",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "RESOLVED"
    assert promise_record.promise_id == "id109"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete == "iku"


def test_case_110_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id110",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id110",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_111_transition_from_resolved_to_resolved_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id111",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id111",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_130_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id130",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id130",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_131_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id131",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id131",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_132_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id132",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id132",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_133_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id133",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id133",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_146_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id146",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id146",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_147_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id147",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id147",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_148_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id148",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id148",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_149_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id149",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id149",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_168_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id168",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id168",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_169_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id169",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id169",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_170_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id170",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id170",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_171_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id171",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    promise_record = store.create(
        promise_id="id171",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "REJECTED"
    assert promise_record.promise_id == "id171"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_172_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id172",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id172",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_173_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id173",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id173",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_186_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id186",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id186",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_187_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id187",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id187",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_188_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id188",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id188",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_189_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id189",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    promise_record = store.create(
        promise_id="id189",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "REJECTED"
    assert promise_record.promise_id == "id189"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete == "iku"


def test_case_190_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id190",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id190",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_191_transition_from_rejected_to_rejected_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id191",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id191",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_210_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id210",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id210",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_211_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id211",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id211",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_212_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id212",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id212",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_213_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id213",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id213",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_226_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id226",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id226",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_227_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id227",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id227",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_228_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id228",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id228",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_229_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id229",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id229",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_248_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id248",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id248",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_249_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id249",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id249",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_250_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id250",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id250",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_251_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id251",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    promise_record = store.create(
        promise_id="id251",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "REJECTED_CANCELED"
    assert promise_record.promise_id == "id251"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_252_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id252",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id252",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_253_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id253",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id253",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_266_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id266",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id266",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_267_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id267",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id267",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_268_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id268",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id268",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_269_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id269",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    promise_record = store.create(
        promise_id="id269",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "REJECTED_CANCELED"
    assert promise_record.promise_id == "id269"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete == "iku"


def test_case_270_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id270",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id270",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_271_transition_from_canceled_to_canceled_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id271",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )

    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id271",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_290_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id290",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id290",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_291_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id291",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id291",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_292_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id292",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id292",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_293_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id293",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id293",
            ikey="ikc",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_306_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id306",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id306",
            ikey=None,
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_307_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id307",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id307",
            ikey=None,
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_308_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id308",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id308",
            ikey="ikc",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_309_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id309",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    promise_record = store.create(
        promise_id="id309",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "REJECTED_TIMEDOUT"
    assert promise_record.promise_id == "id309"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None


def test_case_310_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id310",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id310",
            ikey="ikc*",
            strict=True,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )


def test_case_311_transition_from_timedout_to_timedout_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    store.create(
        promise_id="id311",
        ikey="ikc",
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    with pytest.raises(httpx.HTTPStatusError):
        store.create(
            promise_id="id311",
            ikey="ikc*",
            strict=False,
            headers=None,
            data=None,
            timeout=sys.maxsize,
            tags=None,
        )

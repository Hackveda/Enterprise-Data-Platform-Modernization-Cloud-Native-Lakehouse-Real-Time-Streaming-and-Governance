from pathlib import Path

from mdm.service import CustomerMDM


def test_mdm_create_duplicate_and_update(tmp_path: Path):
    mdm = CustomerMDM(tmp_path / "mdm.sqlite")

    created = mdm.upsert({
        "source_system": "cars24_app",
        "source_customer_id": "C-1001",
        "full_name": "Devanshu Shukla",
        "email": "DEVANSHU@example.com ",
        "phone": "+91 98765 43210",
        "country": "in",
        "address": "Delhi",
    })
    assert created.action == "created"
    assert created.duplicate is False

    duplicate = mdm.upsert({
        "source_system": "call_center",
        "source_customer_id": "LEAD-9",
        "full_name": "Devanshu Shukla",
        "email": "devanshu@example.com",
        "phone": "9876543210",
        "country": "IN",
        "address": "Delhi",
    })
    assert duplicate.master_customer_id == created.master_customer_id
    assert duplicate.duplicate is True
    assert duplicate.match_type == "email_exact"

    updated = mdm.upsert({
        "source_system": "cars24_app",
        "source_customer_id": "C-1001",
        "full_name": "Devanshu Shukla",
        "email": "devanshu@example.com",
        "phone": "9876543210",
        "country": "IN",
        "address": "Noida",
    })
    assert updated.action == "updated"
    assert "address" in updated.changed_fields
    assert updated.golden_record["address"] == "Noida"

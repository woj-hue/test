from process_invoices import validate_totals

def test_validate_ok():
    data = {"total_net": 100.00, "total_vat": 23.00, "total_gross": 123.00, "line_items":[{"net":100.00,"vat_rate":23,"vat":23.00,"gross":123.00}]}
    ok, errs = validate_totals(data, tol=0.01)
    assert ok and not errs

def test_validate_mismatch():
    data = {"total_net": 100.00, "total_vat": 23.00, "total_gross": 123.00, "line_items":[{"net":99.50,"vat_rate":23,"vat":22.89,"gross":122.39}]}
    ok, errs = validate_totals(data, tol=0.01)
    assert not ok and errs

# process_invoices.py

def validate_totals(data, tol=0.01):
    """Minimalna walidacja sum pozycji vs. sumy całkowite.
    Zwraca (ok: bool, errors: list[str]).
    """
    total_net = float(data.get("total_net", 0) or 0)
    total_vat = float(data.get("total_vat", 0) or 0)
    total_gross = float(data.get("total_gross", 0) or 0)

    items = data.get("line_items", []) or []
    net_sum = sum(float(i.get("net", 0) or 0) for i in items)
    vat_sum = sum(float(i.get("vat", 0) or 0) for i in items)
    gross_sum = sum(float(i.get("gross", 0) or 0) for i in items)

    errors = []
    if abs(net_sum - total_net) > tol:
        errors.append(f"Mismatch net: {net_sum:.2f} vs {total_net:.2f}")
    if abs(vat_sum - total_vat) > tol:
        errors.append(f"Mismatch vat: {vat_sum:.2f} vs {total_vat:.2f}")
    if abs(gross_sum - total_gross) > tol:
        errors.append(f"Mismatch gross: {gross_sum:.2f} vs {total_gross:.2f}")

    return (len(errors) == 0, errors)

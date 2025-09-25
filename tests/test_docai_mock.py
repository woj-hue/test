class _Ent:
    def __init__(self, t, txt, props=None):
        self.type_ = t; self.mention_text = txt; self.properties = props or []

def make_doc():
    line = _Ent("line_item","",[
        _Ent("line_item/description","Filet z dorsza"),
        _Ent("line_item/quantity","2"),
        _Ent("line_item/unit_price","10"),
        _Ent("line_item/net_amount","20"),
        _Ent("line_item/tax_rate","23"),
        _Ent("line_item/tax_amount","4.6"),
        _Ent("line_item/amount","24.6"),
    ])
    doc = type("Doc",(),{})()
    doc.entities = [
        _Ent("invoice_id","FV/1/2025"),
        _Ent("supplier_name","Dostawca Sp. z o.o."),
        _Ent("supplier_tax_id","PL1234567890"),
        _Ent("invoice_date","2025-09-10"),
        _Ent("total_net_amount","20"),
        _Ent("total_tax_amount","4.6"),
        _Ent("total_gross_amount","24.6"),
        line,
    ]
    return doc

def test_extract_fields():
    from process_invoices import extract_invoice_fields, validate_totals
    doc = make_doc()
    data = extract_invoice_fields(doc)
    ok, errs = validate_totals(data)
    assert ok, errs
    assert data["invoice_id"] == "FV/1/2025"
    assert data["line_items"][0]["description"].startswith("Filet")

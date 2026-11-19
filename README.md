# Fatawa Razviya — Jild 21 tashreeh v4 merge

Merge workspace: 12 leaf chunk files of the easy-Urdu tashreeh v4 for Fatawa Razviya Jild 21 (volume_id 43; 181 items, 577 pages) are merged into `jild-21.json` by GitHub Actions (`merge_jild21_v4.py`), then published as a v1.0 release asset.

Item order reproduces the v3 merged file exactly (first jild21-pesh-lafz, last jild21-khatima-note). All 12 leaf chunks carry start_page == min(pages). No renames at merge — the 5 print-error renames were already applied in the leaves — and the merge joins 3 simple duplicate-id pairs (masala-166, 180, 203-206) plus the cross-id masala-172-175 (head + shifa-ul-walah tail), then stable-sorts by start_page, matching the v3 merged file. No page markers are present in the chunks.

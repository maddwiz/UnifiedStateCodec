from __future__ import annotations

from usc.slots.typed_slots import (
    infer_slot_types,
    TypedSlotsState,
    pack_params_typed,
    unpack_params_typed,
)

def main():
    # 3 slots: [pid:int, status:str, code:int]
    samples = [
        ["123", "OK", "200"],
        ["124", "OK", "200"],
        ["125", "FAIL", "500"],
        ["126", "OK", "200"],
    ]

    # build samples_by_slot
    samples_by_slot = [[], [], []]
    for ev in samples:
        for i, v in enumerate(ev):
            samples_by_slot[i].append(v)

    slot_types = infer_slot_types(samples_by_slot, int_min_hits=2)
    st = TypedSlotsState.from_types(slot_types)

    blob = pack_params_typed(st, samples)
    print("slot_types:", slot_types)
    print("packed bytes:", len(blob))

    # decode
    slot_types2, decoded = unpack_params_typed(blob, event_count=len(samples))
    print("decoded:")
    for row in decoded:
        print(row)

if __name__ == "__main__":
    main()

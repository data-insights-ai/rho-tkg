| benchmark | before ns/op (median) | after ns/op (median) | delta | B/op before → after | allocs before → after |
|---|---|---|---|---|---|
| PointDoors/memory/NodeAtTx/hot | 238 | 233 | -2.2% | 320 → 320 | 4 → 4 |
| PointDoors/memory/RelAtTx/hot | 244 | 252 | +3.5% | 352 → 352 | 4 → 4 |
| PointDoors/memory/NodeAsOf/hot | 230 | 236 | +3.0% | 320 → 320 | 4 → 4 |
| PointDoors/memory/NodeAtTx/cold | 681 | 638 | -6.4% | 708 → 708 | 13 → 13 |
| PointDoors/memory/RelAtTx/cold | 682 | 738 | +8.2% | 772 → 772 | 13 → 13 |
| PointDoors/memory/NodeAsOf/cold | 370 | 409 | +10.5% | 424 → 424 | 6 → 6 |
| PointDoors/badger/NodeAtTx/hot | 224 | 251 | +11.7% | 324 → 325 | 4 → 4 |
| PointDoors/badger/RelAtTx/hot | 223 | 248 | +11.5% | 353 → 353 | 4 → 4 |
| PointDoors/badger/NodeAsOf/hot | 1474 | 1572 | +6.6% | 1104 → 1104 | 17 → 17 |
| PointDoors/badger/NodeAtTx/cold | 5607 | 5496 | -2.0% | 4264 → 4265 | 61 → 61 |
| PointDoors/badger/RelAtTx/cold | 6162 | 6237 | +1.2% | 5480 → 5481 | 63 → 63 |
| PointDoors/badger/NodeAsOf/cold | 5577 | 5543 | -0.6% | 4278 → 4278 | 61 → 61 |
| MoveWrites/memory/Nodes.Update | 2750 | 2786 | +1.3% | 1922 → 1922 | 24 → 24 |
| MoveWrites/memory/Rels.Update | 3444 | 3494 | +1.5% | 2448 → 2448 | 27 → 27 |
| MoveWrites/badger/Nodes.Update | 7559 | 7942 | +5.1% | 4404 → 4491 | 56 → 56 |
| MoveWrites/badger/Rels.Update | 9163 | 9292 | +1.4% | 5596 → 5632 | 66 → 67 |

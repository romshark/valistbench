# valistbench
Synthetic benchmark comparing the parsing performance of textual value lists in different programming languages.


An implementation must parse a list of labels associated with a given signed 32-bit integer value and calculate the aggregate for every label.
```
A = 56; A = -3; C = 2; B = -700; A = 11; C = -2
```
The above input will therefore need to print the following results in [JSON](https://www.json.org/) to stdout:
```json
{
  "A": {
    "values": 3,
    "value": 64
  },
  "B": {
    "values": 1,
    "value": -700
  },
  "C": {
    "values": 2,
    "value": 0
  }
}
```

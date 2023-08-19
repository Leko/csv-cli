# csv-cli

Command-Line utility for checking statistical features of CSV file(s)

## Install

`npm i csv-cli`

## Usage

```
csv <subcommand> [options...]

Commands:
  csv describe <glob>         Describe statistical features of given CSVs
  csv unique <glob> <column>  Describe statistical features of given CSVs
  csv intersections <glob>    Describe statistical features of given CSVs

Options:
      --help                             Show help                     [boolean]
      --version                          Show version number           [boolean]
  -H, --avoid-using-first-row-as-header               [boolean] [default: false]
```

### describe

```
csv describe example/test.csv
```

```
test (1000 rows)
 column  count  miss  uniq  mean            min         max        std              uniq values
 int     1000   0%    1000  1076603284.784  1000107808  995380654  614202595.32212  995380654 (1, 0.10%)
                                                                                    992123857 (1, 0.10%)
                                                                                    99049246 (1, 0.10%)
                                                                                    987956586 (1, 0.10%)
                                                                                    98565729 (1, 0.10%)
 str     1000   0%    5                                                             apple (226, 22.60%)
                                                                                    grape (205, 20.50%)
                                                                                    strawberry (200, 20.00%)
                                                                                    banana (197, 19.70%)
                                                                                    orange (172, 17.20%)
```

### unique

```
csv unique example/test.csv str
```

```
 value       count  percentage
 apple       226    22.600%
 grape       205    20.500%
 strawberry  200    20.000%
 banana      197    19.700%
 orange      172    17.200%
```

## Development

```
npm i
npx ts-node -T src/cli.ts --help
```

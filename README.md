# dalla-stats-cli

## Requirements
- Julia 1.0
- Packages:
    ```bash
    $ julia
    ```
    ```julia
    using Pkg
    Pkg.add("MySQL")
    Pkg.add("DataFrames")
    ```

## Usage
```bash
$ julia
```

```julia
using MySQL
using DataFrames
conn = MySQL.connect("127.0.0.1", "dalla", "password", port = 13306, db = "dalla_stats")

delta_df = calculate_delta(conn, month)
device_df = create_device_table(delta_df)

# Apply update
update_device_table(conn, device_df)

# View update
new_table = MySQL.query(conn, """SELECT * FROM device;""", DataFrame)
println(new_table)

# Further analysis on delta_df...
```

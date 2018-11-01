module DallaStats
import Dates
using MySQL
using DataFrames

# Calculate delta for each device

"""
    unixtime(month::Integer)

Return the unix time for the start of the month [1, 12] as UTC+02
"""
function month_local_unix_time(month::Integer)::Int64
    year = Dates.year(Dates.now())
    month_start = Dates.DateTime(year, month, 1, 2)
    convert(Int64, Dates.datetime2unix(month_start))
end

function classify_row_delta!(row)
    record_time = Dates.unix2datetime(row[:record_time])
    h = Dates.hour(record_time)

    # Local Time: (UTC+02): [0, 6)
    # UTC   Time: (UTC): [22, 4)
    # On peak = h >= 4 && h < 22
    if h >= (6 - 2) && h < (24 - 2)
        row[:off_peak] = 0
        row[:on_peak] = row[:delta]
    else
        row[:off_peak] = row[:delta]
        row[:on_peak] = 0
    end
end

function calculate_row_delta(prev_row, current_row)
    if prev_row != Nothing
        delta = current_row[:total_bytes] - prev_row[:total_bytes]

        if delta < 0
            delta = current_row[:total_bytes]
        end
    else
        delta = 0
    end

    delta
end

"""
    calculate_delta(conn, month)

Return the history table after calculating delta values
"""
function calculate_delta(conn, month)
    start = month_local_unix_time(month)

    println("Fetching history table rows after ", Dates.unix2datetime(start), " UTC")
    history = MySQL.query(conn, """SELECT * FROM history WHERE record_time >= $start;""", DataFrame)

    delete!(history, :ip_address)
    device_ids = unique(history[:device_id])

    println("rows: ", length(history[:id]))

    delta_history = DataFrame(id = Int32[], device_id = Int32[],
    record_time = Int32[], total_bytes = Int64[], delta = Int64[],
    on_peak = Int64[], off_peak = Int64[])

    device_counter = 1
    for device_id in device_ids
        println("device ", device_counter, "/", length(device_ids))

        device_df = sort(history[history.device_id .== device_id, :], :record_time)
        device_df[:delta] = Vector{Int64}(undef, length(device_df[:id]))
        device_df[:on_peak] = Vector{Int64}(undef, length(device_df[:id]))
        device_df[:off_peak] = Vector{Int64}(undef, length(device_df[:id]))

        prev_row = Nothing
        for row in eachrow(device_df)
            row[:delta] = calculate_row_delta(prev_row, row)
            prev_row = row
            classify_row_delta!(row)
        end

        delta_history = vcat(delta_history, device_df)
        device_counter = device_counter + 1
    end

    delta_history
end

function create_device_table(delta_df)
    by(delta_df, :device_id) do df
        DataFrame(total_bytes = sort(df, :record_time)[end, :total_bytes], on_peak = sum(df.on_peak), off_peak=sum(df.off_peak))
    end
end

function update_device_table(conn, device_df)
    println("Updating device table...")
    MySQL.execute!(conn, """UPDATE device SET on_peak = 0, off_peak = 0;""")

    update_stmt = MySQL.Stmt(conn, """UPDATE device SET total_bytes = ?, on_peak = ?, off_peak = ? WHERE id = ?;""")

    for row in eachrow(device_df)
        MySQL.execute!(update_stmt, [row[:total_bytes], row[:on_peak], row[:off_peak], row[:device_id]])
    end
    println("Successfully updated device table")
end

function update_device_table(conn, month::Integer)
    delta_df = calculate_delta(conn, month)
    device_df = create_device_table(delta_df)
    update_device_table(conn, device_df)

    new_table = MySQL.query(conn, """SELECT * FROM device;""", DataFrame)
    println(new_table)
end

end

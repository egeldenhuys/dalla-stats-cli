using MySQL
using DataFrames

conn = Nothing

if length(ARGS) !=
    println("Usage:\n\tjulia -L cli.jl <host> <username> <password> <port> <database>")
    println("""\nOR\n    conn = MySQL.connect("127.0.0.1", "dalla", "password", port = 3306, db = "dalla_stats")""")
else
    conn = MySQL.connect(ARGS[1], ARGS[2], ARGS[3], port = ARGS[4], db = ARGS[5])
end

include("DallaStats.jl")

println("Usage:\n\tDallaStats.update_device_table(conn, month::Integer)\n\n")
# DallaStats.update_device_table(conn, 11)

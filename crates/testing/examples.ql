put into users
    first_name = "Jacob"
    last_name = "Walton"
    email = "jacob@example.com"
    address = {
        city = "Manchester",
        country = "United Kingdom"
    }
    tags = ["beta", "premium"]
    created_at = now()

---

change users
    where
        address.city = "Manchester"
        tags has "beta"
    set
        status = "active"
        last_modified = now()
        tags = tags + ["verified"]

---

remove from users
    where
        created_at < "2020-01-01"
        status = "inactive"

---

get from users
    where
        address.city = "Manchester"

---

make table users
    as relational
    fields
        id int primary key
        first_name text
        last_name text
        email text unique
        created_at date default now()
        status text default "inactive"

---

make table sessions
    as keyspace
    ttl 3600
    persistence "memory"
    prefix "user:"

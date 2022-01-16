# Module 5: Message Schema

```text
customer
├── address_id: UUID
├── user_id: Option<UUID>
├── payment: String
└── order: Option<Order>
    ├── datetime: Long
    ├── id: UUID
    └──[ dishes ]
        └── dish: Dish
            ├── id: UUID
            ├── name: String
            ├── price: Money (Int + Currency code) 
            ├── quantity: Int
            └──[ ingredients ]
                └── ingredient: Ingredient 
                    ├── id: UUID
                    ├── name: String
                    ├── quantity: Int
                    └── unit: Enum
```

```text
├── user_id: Option<UUID>
├── order_id: Option<UUID>
├── total: String
├── comment: String
└── flag: Enum
```
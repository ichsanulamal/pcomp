with
    final as (
        select *
        from
            (
                select *
                from {{ ref("enterkomputer") }}
                union all
                select *
                from {{ ref("viraindo") }}
            )
        qualify
            row_number() over (
                partition by product_name, category, price, `source`
                order by inserted_at asc nulls last
            )
            = 1
    )

select
    *,
    (
        (
            price - lag(price) over (
                partition by product_name, category, source order by inserted_at
            )
        ) / lag(price) over (
            partition by product_name, category, source order by inserted_at
        )
    )
    * 100 as percentage_change
from final

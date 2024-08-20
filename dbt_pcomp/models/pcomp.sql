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
    CASE 
        -- CPU
        WHEN LOWER(product_name) LIKE '%amd ryzen 3%' THEN 'AMD Ryzen 3 Series'
        WHEN LOWER(product_name) LIKE '%amd ryzen 5%' THEN 'AMD Ryzen 5 Series'
        WHEN LOWER(product_name) LIKE '%amd ryzen 7%' THEN 'AMD Ryzen 7 Series'
        WHEN LOWER(product_name) LIKE '%amd ryzen 9%' THEN 'AMD Ryzen 9 Series'
        WHEN LOWER(product_name) LIKE '%intel core i3%' THEN 'Intel Core i3 Series'
        WHEN LOWER(product_name) LIKE '%intel core i5%' THEN 'Intel Core i5 Series'
        WHEN LOWER(product_name) LIKE '%intel core i7%' THEN 'Intel Core i7 Series'
        WHEN LOWER(product_name) LIKE '%intel core i9%' THEN 'Intel Core i9 Series'

        -- RAM
        WHEN LOWER(product_name) LIKE '%ddr4%' AND LOWER(product_name) LIKE '%2x4gb%' THEN 'DDR4 8GB (2x4GB)'
        WHEN LOWER(product_name) LIKE '%ddr4%' AND LOWER(product_name) LIKE '%2x8gb%' THEN 'DDR4 16GB (2x8GB)'
        WHEN LOWER(product_name) LIKE '%ddr4%' AND LOWER(product_name) LIKE '%2x16gb%' THEN 'DDR4 32GB (2x16GB)'
        WHEN LOWER(product_name) LIKE '%ddr5%' AND LOWER(product_name) LIKE '%2x16gb%' THEN 'DDR5 32GB (2x16GB)'

        -- MONITOR
        -- 23" - 26" Monitors
        WHEN LOWER(product_name) LIKE '%23" - 26%", 1920 x 1080%' THEN '23-26" FHD (1920x1080)'
        WHEN LOWER(product_name) LIKE '%23" - 26%", 2560 x 1440%' THEN '23-26" QHD (2560x1440)'
        
        -- 27" - 30" Monitors
        WHEN LOWER(product_name) LIKE '%27" - 30%", 1920 x 1080%' THEN '27-30" FHD (1920x1080)'
        WHEN LOWER(product_name) LIKE '%27" - 30%", 2560 x 1440%' THEN '27-30" QHD (2560x1440)'
        WHEN LOWER(product_name) LIKE '%27" - 30%", 3840 x 2160%' THEN '27-30" UHD (3840x2160)'
        WHEN LOWER(product_name) LIKE '%27" - 30%", 2560 x 1080%' THEN '27-30" UltraWide FHD (2560x1080)'

        -- 32"+ Monitors
        WHEN LOWER(product_name) LIKE '%32"+%, 2560 x 1440%' THEN '32"+ QHD (2560x1440)'
        WHEN LOWER(product_name) LIKE '%32"+%, 3840 x 2160%' THEN '32"+ UHD (3840x2160)'
        WHEN LOWER(product_name) LIKE '%32"+%, 2560 x 1080%' THEN '32"+ UltraWide FHD (2560x1080)'
        WHEN LOWER(product_name) LIKE '%32"+%, 3440 x 1440%' THEN '32"+ UltraWide QHD (3440x1440)'

        -- POWER SUPPLY
        -- < 500W PSUs
        WHEN LOWER(product_name) LIKE '%< 500w%' AND LOWER(product_name) LIKE '%80+ bronze%' THEN '< 500W 80+ Bronze'
        WHEN LOWER(product_name) LIKE '%< 500w%' AND LOWER(product_name) LIKE '%80+ gold%' THEN '< 500W 80+ Gold'
        WHEN LOWER(product_name) LIKE '%< 500w%' AND LOWER(product_name) LIKE '%80+ platinum%' THEN '< 500W 80+ Platinum'

        -- 500W - 799W PSUs
        WHEN LOWER(product_name) LIKE '%500w - 799w%' AND LOWER(product_name) LIKE '%80+ bronze%' THEN '500W - 799W 80+ Bronze'
        WHEN LOWER(product_name) LIKE '%500w - 799w%' AND LOWER(product_name) LIKE '%80+ gold%' THEN '500W - 799W 80+ Gold'
        WHEN LOWER(product_name) LIKE '%500w - 799w%' AND LOWER(product_name) LIKE '%80+ platinum%' THEN '500W - 799W 80+ Platinum'

        -- 800W - 999W PSUs
        WHEN LOWER(product_name) LIKE '%800w - 999w%' AND LOWER(product_name) LIKE '%80+ bronze%' THEN '800W - 999W 80+ Bronze'
        WHEN LOWER(product_name) LIKE '%800w - 999w%' AND LOWER(product_name) LIKE '%80+ gold%' THEN '800W - 999W 80+ Gold'
        WHEN LOWER(product_name) LIKE '%800w - 999w%' AND LOWER(product_name) LIKE '%80+ platinum%' THEN '800W - 999W 80+ Platinum'

        -- 1000W - 1199W PSUs
        WHEN LOWER(product_name) LIKE '%1000w - 1199w%' AND LOWER(product_name) LIKE '%80+ bronze%' THEN '1000W - 1199W 80+ Bronze'
        WHEN LOWER(product_name) LIKE '%1000w - 1199w%' AND LOWER(product_name) LIKE '%80+ gold%' THEN '1000W - 1199W 80+ Gold'
        WHEN LOWER(product_name) LIKE '%1000w - 1199w%' AND LOWER(product_name) LIKE '%80+ platinum%' THEN '1000W - 1199W 80+ Platinum'

        -- 1200W - 1499W PSUs
        WHEN LOWER(product_name) LIKE '%1200w - 1499w%' AND LOWER(product_name) LIKE '%80+ gold%' THEN '1200W - 1499W 80+ Gold'
        WHEN LOWER(product_name) LIKE '%1200w - 1499w%' AND LOWER(product_name) LIKE '%80+ platinum%' THEN '1200W - 1499W 80+ Platinum'

        -- 1500W - 2000W PSUs
        WHEN LOWER(product_name) LIKE '%1500w - 2000w%' AND LOWER(product_name) LIKE '%80+ gold%' THEN '1500W - 2000W 80+ Gold'
        WHEN LOWER(product_name) LIKE '%1500w - 2000w%' AND LOWER(product_name) LIKE '%80+ platinum%' THEN '1500W - 2000W 80+ Platinum'

        -- STORAGE
        -- 2.5" SATA Solid State Drives
        WHEN LOWER(product_name) LIKE '%solid state drive - 2.5" sata 128 gb%' THEN 'SSD 2.5" SATA 128 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - 2.5" sata 256 gb%' THEN 'SSD 2.5" SATA 256 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - 2.5" sata 512 gb%' THEN 'SSD 2.5" SATA 512 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - 2.5" sata 1 tb%' THEN 'SSD 2.5" SATA 1 TB'
        WHEN LOWER(product_name) LIKE '%solid state drive - 2.5" sata 2 tb%' THEN 'SSD 2.5" SATA 2 TB'

        -- M.2 SATA Solid State Drives
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 sata 128 gb%' THEN 'SSD M.2 SATA 128 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 sata 256 gb%' THEN 'SSD M.2 SATA 256 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 sata 512 gb%' THEN 'SSD M.2 SATA 512 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 sata 1 tb%' THEN 'SSD M.2 SATA 1 TB'

        -- M.2 NVMe Solid State Drives
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 nvme 128 gb%' THEN 'SSD M.2 NVMe 128 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 nvme 256 gb%' THEN 'SSD M.2 NVMe 256 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 nvme 512 gb%' THEN 'SSD M.2 NVMe 512 GB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 nvme 1 tb%' THEN 'SSD M.2 NVMe 1 TB'
        WHEN LOWER(product_name) LIKE '%solid state drive - m.2 nvme 2 tb%' THEN 'SSD M.2 NVMe 2 TB'

        -- 3.5" SATA Hard Drives
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 1 tb%' THEN 'HDD 3.5" SATA 1 TB'
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 2 tb%' THEN 'HDD 3.5" SATA 2 TB'
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 4 tb%' THEN 'HDD 3.5" SATA 4 TB'
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 6 tb%' THEN 'HDD 3.5" SATA 6 TB'
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 8 tb%' THEN 'HDD 3.5" SATA 8 TB'
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 10 tb%' THEN 'HDD 3.5" SATA 10 TB'
        WHEN LOWER(product_name) LIKE '%hard drive - 3.5" sata 12 tb%' THEN 'HDD 3.5" SATA 12 TB'

        -- VGA
        -- Intel Arc Series
        WHEN LOWER(product_name) LIKE '%arc a380%' THEN 'Intel Arc A380'
        WHEN LOWER(product_name) LIKE '%arc a750%' THEN 'Intel Arc A750'
        WHEN LOWER(product_name) LIKE '%arc a770%' THEN 'Intel Arc A770'
        
        -- NVIDIA GeForce RTX 30 Series
        WHEN LOWER(product_name) LIKE '%geforce rtx 3050%' THEN 'NVIDIA GeForce RTX 3050'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3060 12gb%' THEN 'NVIDIA GeForce RTX 3060 12GB'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3060 ti lhr%' THEN 'NVIDIA GeForce RTX 3060 Ti LHR'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3060 ti%' THEN 'NVIDIA GeForce RTX 3060 Ti'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3070 lhr%' THEN 'NVIDIA GeForce RTX 3070 LHR'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3070%' THEN 'NVIDIA GeForce RTX 3070'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3070 ti%' THEN 'NVIDIA GeForce RTX 3070 Ti'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3080 12gb lhr%' THEN 'NVIDIA GeForce RTX 3080 12GB LHR'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3080 10gb lhr%' THEN 'NVIDIA GeForce RTX 3080 10GB LHR'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3080 10gb%' THEN 'NVIDIA GeForce RTX 3080 10GB'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3080 ti%' THEN 'NVIDIA GeForce RTX 3080 Ti'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3090 ti%' THEN 'NVIDIA GeForce RTX 3090 Ti'
        WHEN LOWER(product_name) LIKE '%geforce rtx 3090%' THEN 'NVIDIA GeForce RTX 3090'
        
        -- NVIDIA GeForce RTX 40 Series
        WHEN LOWER(product_name) LIKE '%geforce rtx 4070 ti super%' THEN 'NVIDIA GeForce RTX 4070 Ti SUPER'
        WHEN LOWER(product_name) LIKE '%geforce rtx 4070 super%' THEN 'NVIDIA GeForce RTX 4070 SUPER'
        WHEN LOWER(product_name) LIKE '%geforce rtx 4070 ti%' THEN 'NVIDIA GeForce RTX 4070 Ti'
        WHEN LOWER(product_name) LIKE '%geforce rtx 4070%' THEN 'NVIDIA GeForce RTX 4070'
        WHEN LOWER(product_name) LIKE '%geforce rtx 4080 super%' THEN 'NVIDIA GeForce RTX 4080 SUPER'
        WHEN LOWER(product_name) LIKE '%geforce rtx 4080%' THEN 'NVIDIA GeForce RTX 4080'
        WHEN LOWER(product_name) LIKE '%geforce rtx 4090%' THEN 'NVIDIA GeForce RTX 4090'
        
        -- AMD Radeon RX 6000 Series
        WHEN LOWER(product_name) LIKE '%radeon rx 6500 xt%' THEN 'AMD Radeon RX 6500 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 6600 xt%' THEN 'AMD Radeon RX 6600 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 6600%' THEN 'AMD Radeon RX 6600'
        WHEN LOWER(product_name) LIKE '%radeon rx 6700 xt%' THEN 'AMD Radeon RX 6700 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 6800 xt%' THEN 'AMD Radeon RX 6800 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 6800%' THEN 'AMD Radeon RX 6800'
        WHEN LOWER(product_name) LIKE '%radeon rx 6900 xt%' THEN 'AMD Radeon RX 6900 XT'
        
        -- AMD Radeon RX 7000 Series
        WHEN LOWER(product_name) LIKE '%radeon rx 7700 xt%' THEN 'AMD Radeon RX 7700 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 7800 xt%' THEN 'AMD Radeon RX 7800 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 7900 xtx%' THEN 'AMD Radeon RX 7900 XTX'
        WHEN LOWER(product_name) LIKE '%radeon rx 7900 xt%' THEN 'AMD Radeon RX 7900 XT'
        WHEN LOWER(product_name) LIKE '%radeon rx 7900 gre%' THEN 'AMD Radeon RX 7900 GRE'

        ELSE 'Other'
    END AS product_general_name,
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

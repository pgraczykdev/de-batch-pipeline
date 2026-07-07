SELECT 
    channel_id,
    channel_desc,
    channel_class
FROM {{ source('staging', 'channels') }}
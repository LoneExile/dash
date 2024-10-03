CREATE TABLE IF NOT EXISTS appendix (
    id text PRIMARY KEY,
    prefix text,
    created_at timestamp,
    chapters text
    {{ CUSTOM_APPENDIX }}
)

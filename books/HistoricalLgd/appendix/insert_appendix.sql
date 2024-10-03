INSERT
    OR REPLACE INTO appendix (id, prefix, created_at, chapters {{ CUSTOM_APPENDIX_KEY }})
        VALUES (
    '{{ ID_APPENDIX }}',
    '{{ PREFIX_APPENDIX }}',
    '{{ CREATED_AT_APPENDIX }}',
    '{{ CHAPTERS_APPENDIX}}' {{ CUSTOM_APPENDIX_VALUE }}
    )

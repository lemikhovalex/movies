"""Password validation.

https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators
"""

_NAME = "NAME"
_dj_contrib_au_p_val = "django.contrib.auth.password_validation"

AUTH_PASSWORD_VALIDATORS = [
    {
        _NAME: "".join(
            [
                _dj_contrib_au_p_val,
                ".UserAttributeSimilarityValidator",
            ],
        ),
    },
    {
        _NAME: "".join(
            [
                _dj_contrib_au_p_val,
                ".MinimumLengthValidator",
            ],
        ),
    },
    {
        _NAME: "".join(
            [
                _dj_contrib_au_p_val,
                ".CommonPasswordValidator",
            ],
        ),
    },
    {
        _NAME: "".join(
            [
                _dj_contrib_au_p_val,
                ".NumericPasswordValidator",
            ],
        ),
    },
]

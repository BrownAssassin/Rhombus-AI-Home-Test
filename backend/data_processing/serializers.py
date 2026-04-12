"""API serializers for request validation."""

from rest_framework import serializers

from .services.inference import ALLOWED_OVERRIDE_TYPES


class S3CredentialsSerializer(serializers.Serializer):
    """Validate the runtime S3 credentials and bucket context."""

    access_key_id = serializers.CharField(max_length=256, trim_whitespace=True)
    secret_access_key = serializers.CharField(max_length=256, trim_whitespace=True)
    session_token = serializers.CharField(
        max_length=4096,
        trim_whitespace=True,
        required=False,
        allow_blank=True,
    )
    region = serializers.CharField(max_length=64, trim_whitespace=True)
    bucket = serializers.CharField(max_length=255, trim_whitespace=True)
    prefix = serializers.CharField(
        max_length=1024,
        trim_whitespace=True,
        required=False,
        allow_blank=True,
    )


class ListFilesRequestSerializer(S3CredentialsSerializer):
    """Request body for listing supported S3 files."""

    pass


class ColumnOverrideSerializer(serializers.Serializer):
    """Manual column type override requested by the frontend."""

    column = serializers.CharField(max_length=255, trim_whitespace=True)
    target_type = serializers.ChoiceField(choices=ALLOWED_OVERRIDE_TYPES)


class ProcessFileRequestSerializer(S3CredentialsSerializer):
    """Request body for processing a selected S3 object."""

    object_key = serializers.CharField(max_length=1024, trim_whitespace=True)
    sheet_name = serializers.CharField(
        max_length=255,
        trim_whitespace=True,
        required=False,
        allow_blank=True,
    )
    preview_row_limit = serializers.IntegerField(required=False, min_value=1, max_value=500, default=100)
    overrides = ColumnOverrideSerializer(many=True, required=False)


class PreviewPageRequestSerializer(S3CredentialsSerializer):
    """Request body for loading a later processed preview page."""

    run_id = serializers.IntegerField(min_value=1, required=False)
    object_key = serializers.CharField(max_length=1024, trim_whitespace=True, required=False)
    file_type = serializers.ChoiceField(choices=["csv", "excel"], required=False)
    selected_sheet = serializers.CharField(
        max_length=255,
        trim_whitespace=True,
        required=False,
        allow_blank=True,
    )
    row_count = serializers.IntegerField(required=False, min_value=0)
    schema = serializers.ListField(child=serializers.JSONField(), required=False)
    preview_columns = serializers.ListField(child=serializers.CharField(max_length=255), required=False)
    page = serializers.IntegerField(required=False, min_value=1, default=1)
    page_size = serializers.IntegerField(required=False, min_value=1, max_value=500, default=100)

    def validate(self, attrs):
        """Require either a saved run id or enough context to page statelessly."""

        has_preview_context = all(field in attrs for field in ("object_key", "file_type", "row_count", "schema"))
        if "run_id" not in attrs and not has_preview_context:
            raise serializers.ValidationError(
                "Provide either a run_id or the current preview context to load another page."
            )
        return attrs

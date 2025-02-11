schema_version = 1

project {
  license        = "MPL-2.0"
  copyright_year = 2013

  # (OPTIONAL) A list of globs that should not have copyright/license headers.
  # Supports doublestar glob patterns for more flexibility in defining which
  # files or folders should be ignored
  header_ignore = [
    # Forked and modified UI libs
    "ui/packages/consul-ui/app/utils/dom/event-target/**",
    "ui/packages/consul-ui/lib/rehype-prism/**",
    "ui/packages/consul-ui/lib/block-slots/**",
  ]
}

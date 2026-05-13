declare module 'virtual:felderaApiJsonSchemas.json' {
  // eslint-disable-next-line
  const module: {
    readonly uri: string
    readonly fileMatch?: string[]
    readonly schema?: any
  }[]
  export default module
}

declare module 'virtual:feldera-triage-plugins' {
  import type { ZipItem } from 'but-unzip'
  import type { TriagePlugin, DecodedBundle } from 'triage-types'
  export { TriageResults } from 'triage-types'
  export function createBundle(files: ZipItem[]): Promise<DecodedBundle>
  const plugins: TriagePlugin[]
  export default plugins
}

declare module 'virtual:opendera-cloud-chrome' {
  import type { Component } from 'svelte'
  // Null in OSS builds; populated by the cloud build via
  // OPENDERA_CLOUD_CHROME_MODULE. Props mirror the plugin's
  // TenantSwitcher.svelte signature.
  export const TenantSwitcher:
    | Component<{ current_tenant_id: string }>
    | null
}

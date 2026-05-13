<script lang="ts">
  import OpenderaLogoColorDark from '$assets/images/opendera/Opendera Logo Color Dark.svg?component'
  import OpenderaLogoColorLight from '$assets/images/opendera/Opendera Logo Color Light.svg?component'
  import OpenderaLogomarkColorDark from '$assets/images/opendera/Opendera Logomark Color Dark.svg?component'
  import OpenderaLogomarkColorLight from '$assets/images/opendera/Opendera Logomark Color Light.svg?component'
  import ProfileButton from '$lib/components/auth/ProfileButton.svelte'
  import { useClusterHealth } from '$lib/compositions/health/useClusterHealth.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { resolve } from '$lib/functions/svelte'
  import { page } from '$app/state'
  import type { Snippet } from '$lib/types/svelte'
  // Null in OSS builds. The cloud build's vite config rewires this
  // virtual module to the console-plugin's TenantSwitcher.
  import { TenantSwitcher } from 'virtual:opendera-cloud-chrome'

  const { afterStart, beforeEnd }: { afterStart?: Snippet; beforeEnd?: Snippet } = $props()
  const darkMode = useDarkMode()

  const healthStatus = useClusterHealth()

  // The current tenant id is exposed via the layout data the OSS
  // console already loads. Cloud chrome only renders when both the
  // build flag is set and the manager reports a tenant.
  const tenantId = $derived(page.data.feldera?.tenantId ?? '')
</script>

<div class="flex flex-row items-center justify-between gap-4 px-2 py-2 md:px-8">
  <a class="py-3 lg:pt-2 lg:pr-6 lg:pb-4" href={resolve('/')}>
    <span class="hidden lg:block">
      {#if darkMode.current === 'dark'}
        <OpenderaLogoColorLight class="h-8"></OpenderaLogoColorLight>
      {:else}
        <OpenderaLogoColorDark class="h-8"></OpenderaLogoColorDark>
      {/if}
    </span>
    <span class="inline lg:hidden">
      {#if darkMode.current === 'dark'}
        <OpenderaLogomarkColorLight class="h-8"></OpenderaLogomarkColorLight>
      {:else}
        <OpenderaLogomarkColorDark class="h-8"></OpenderaLogomarkColorDark>
      {/if}
    </span>
  </a>
  {#if TenantSwitcher && tenantId}
    <TenantSwitcher current_tenant_id={tenantId} />
  {/if}
  {@render afterStart?.()}
  <!-- <div class="flex flex-1"></div> -->
  <div class="-mr-4 ml-auto"></div>
  {@render beforeEnd?.()}
  <ProfileButton compactBreakpoint="xl:" healthStatus={healthStatus.current}></ProfileButton>
</div>

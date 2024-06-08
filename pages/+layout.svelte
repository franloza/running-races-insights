<script>
	import '@evidence-dev/tailwind/fonts.css';
	import '../app.css';
	import { EvidenceDefaultLayout } from '@evidence-dev/core-components';
	export let data;

let SiteColors = [
        '#cf0d06',
        '#eb5752',
        '#e88a87',
        '#fcdad9',
]
</script>

<EvidenceDefaultLayout {data} neverShowQueries={true} fullWidth={false} githubRepo="https://github.com/franloza/running-races-insights" xProfile="https://x.com/franloza93" hideSidebar={true} title="Estadísticas del Circuito de Carreras Populares de Cuenca">
	<slot slot="content" />
</EvidenceDefaultLayout>
package main

import (
	"fmt"
	"testing"

	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
	"github.com/stretchr/testify/assert"
)

var m_a_p map[string]interface{}

// init setups common data used in tests and benchmarks (same main func in test.go)
func init() {
	// A Base64 Encoded TSV Snowplow Payload, which includes JSON in some columns
	// We'll use Snowplow Analytics SDK to parse the parts we want.
	// We are interested in the derived contexts ... see below
	data := "Y2hyb21lLWRlYnVnZ2VyLWRldgl3ZWIJMjAyMS0wNy0yNyAyMToyNToyNS44MzQJMjAyMS0wNy0yNyAyMToyNToyMy4xOTgJMjAyMS0wNy0yNyAyMToyNToyMy4wNDcJc3RydWN0CTA3ZTg4NzZmLTQ0NzUtNDIyOS05ZTJjLTcwNThkNmJiMjc2MwkJc3AJanMtMi4xNC4wCXNzYy0yLjMuMC1raW5lc2lzCXN0cmVhbS0yLjAuMS1jb21tb24tMi4wLjEJCTc1LjgwLjExMC4xODYJCTljZTVhMGI2LTkyYjctNGZmMS05M2ZjLTEwNDc5YWZkYTFlZAkxCWYyOGJkNTJiLTliM2ItNDliYi04ZTIzLTM3YmU4ODEzODQ5MgkJCQkJCQkJCQkJCWh0dHBzOi8vZnJlc2gtY293LTM3LmxvY2EubHQvc25vd3Bsb3d2Mi5odG1sCQlodHRwczovL2ZyZXNoLWNvdy0zNy5sb2NhLmx0LwlodHRwcwlmcmVzaC1jb3ctMzcubG9jYS5sdAk0NDMJL3Nub3dwbG93djIuaHRtbAkJCWh0dHBzCWZyZXNoLWNvdy0zNy5sb2NhLmx0CTQ0MwkvCQkJaW50ZXJuYWwJCQkJCQkJCXsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvY29udGV4dHMvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOlt7InNjaGVtYSI6ImlnbHU6Y29tLmdvb2dsZS5hbmFseXRpY3MvY29va2llcy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6e319LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvd2ViX3BhZ2UvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsiaWQiOiJhZmQ0YjkyYy1jZTgxLTQwMjctOTNiMy1jMWE0MGNlYjYwNzQifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy53My9QZXJmb3JtYW5jZVRpbWluZy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYXZpZ2F0aW9uU3RhcnQiOjE2Mjc0MjExMDc1MTksInVubG9hZEV2ZW50U3RhcnQiOjE2Mjc0MjExMDc2OTMsInVubG9hZEV2ZW50RW5kIjoxNjI3NDIxMTA3Njk0LCJyZWRpcmVjdFN0YXJ0IjowLCJyZWRpcmVjdEVuZCI6MCwiZmV0Y2hTdGFydCI6MTYyNzQyMTEwNzUyMSwiZG9tYWluTG9va3VwU3RhcnQiOjE2Mjc0MjExMDc1MjEsImRvbWFpbkxvb2t1cEVuZCI6MTYyNzQyMTEwNzUyMSwiY29ubmVjdFN0YXJ0IjoxNjI3NDIxMTA3NTIxLCJjb25uZWN0RW5kIjoxNjI3NDIxMTA3NTIxLCJzZWN1cmVDb25uZWN0aW9uU3RhcnQiOjAsInJlcXVlc3RTdGFydCI6MTYyNzQyMTEwNzUyMywicmVzcG9uc2VTdGFydCI6MTYyNzQyMTEwNzY4NywicmVzcG9uc2VFbmQiOjE2Mjc0MjExMDc2ODgsImRvbUxvYWRpbmciOjE2Mjc0MjExMDc2OTUsImRvbUludGVyYWN0aXZlIjoxNjI3NDIxMTA4MDExLCJkb21Db250ZW50TG9hZGVkRXZlbnRTdGFydCI6MTYyNzQyMTEwODAxMSwiZG9tQ29udGVudExvYWRlZEV2ZW50RW5kIjoxNjI3NDIxMTA4MDEyLCJkb21Db21wbGV0ZSI6MTYyNzQyMTEwODE3MSwibG9hZEV2ZW50U3RhcnQiOjE2Mjc0MjExMDgxNzEsImxvYWRFdmVudEVuZCI6MTYyNzQyMTEwODE3MX19XX0JY2F0ZWdvcnkJYWN0aW9uCWxhYmVsCXByb3BlcnR5CTEwCQkJCQkJCQkJCQkJCQkJCQkJCQlNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMF8xNV83KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvOTIuMC40NTE1LjEwNyBTYWZhcmkvNTM3LjM2CQkJCQkJZW4tVVMJMQkwCTAJMAkwCTAJMAkwCTAJMQkyNAkxOTE3CTUxOAkJCQlBbWVyaWNhL0xvc19BbmdlbGVzCQkJMTkyMAkxMjAwCVVURi04CTE5MTcJNTQ2CQkJCQkJCQkJCQkJMjAyMS0wNy0yNyAyMToyNToyMy4wNDkJCQl7InNjaGVtYSI6ImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L2NvbnRleHRzL2pzb25zY2hlbWEvMS0wLTEiLCJkYXRhIjpbeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91YV9wYXJzZXJfY29udGV4dC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJ1c2VyYWdlbnRGYW1pbHkiOiJDaHJvbWUiLCJ1c2VyYWdlbnRNYWpvciI6IjkyIiwidXNlcmFnZW50TWlub3IiOiIwIiwidXNlcmFnZW50UGF0Y2giOiI0NTE1IiwidXNlcmFnZW50VmVyc2lvbiI6IkNocm9tZSA5Mi4wLjQ1MTUiLCJvc0ZhbWlseSI6Ik1hYyBPUyBYIiwib3NNYWpvciI6IjEwIiwib3NNaW5vciI6IjE1Iiwib3NQYXRjaCI6IjciLCJvc1BhdGNoTWlub3IiOm51bGwsIm9zVmVyc2lvbiI6Ik1hYyBPUyBYIDEwLjE1LjciLCJkZXZpY2VGYW1pbHkiOiJPdGhlciJ9fSx7InNjaGVtYSI6ImlnbHU6bmwuYmFzamVzL3lhdWFhX2NvbnRleHQvanNvbnNjaGVtYS8xLTAtMiIsImRhdGEiOnsiZGV2aWNlQnJhbmQiOiJBcHBsZSIsImRldmljZU5hbWUiOiJBcHBsZSBNYWNpbnRvc2giLCJvcGVyYXRpbmdTeXN0ZW1WZXJzaW9uTWFqb3IiOiIxMCIsImxheW91dEVuZ2luZU5hbWVWZXJzaW9uIjoiQmxpbmsgOTIuMCIsIm9wZXJhdGluZ1N5c3RlbU5hbWVWZXJzaW9uIjoiTWFjIE9TIFggMTAuMTUuNyIsImxheW91dEVuZ2luZU5hbWVWZXJzaW9uTWFqb3IiOiJCbGluayA5MiIsIm9wZXJhdGluZ1N5c3RlbU5hbWUiOiJNYWMgT1MgWCIsImFnZW50VmVyc2lvbk1ham9yIjoiOTIiLCJsYXlvdXRFbmdpbmVWZXJzaW9uTWFqb3IiOiI5MiIsImRldmljZUNsYXNzIjoiRGVza3RvcCIsImFnZW50TmFtZVZlcnNpb25NYWpvciI6IkNocm9tZSA5MiIsIm9wZXJhdGluZ1N5c3RlbU5hbWVWZXJzaW9uTWFqb3IiOiJNYWMgT1MgWCAxMCIsImRldmljZUNwdUJpdHMiOiIzMiIsIm9wZXJhdGluZ1N5c3RlbUNsYXNzIjoiRGVza3RvcCIsImxheW91dEVuZ2luZU5hbWUiOiJCbGluayIsImFnZW50TmFtZSI6IkNocm9tZSIsImFnZW50VmVyc2lvbiI6IjkyLjAuNDUxNS4xMDciLCJsYXlvdXRFbmdpbmVDbGFzcyI6IkJyb3dzZXIiLCJhZ2VudE5hbWVWZXJzaW9uIjoiQ2hyb21lIDkyLjAuNDUxNS4xMDciLCJvcGVyYXRpbmdTeXN0ZW1WZXJzaW9uIjoiMTAuMTUuNyIsImRldmljZUNwdSI6IkludGVsIiwiYWdlbnRDbGFzcyI6IkJyb3dzZXIiLCJsYXlvdXRFbmdpbmVWZXJzaW9uIjoiOTIuMCJ9fSx7InNjaGVtYSI6ImlnbHU6b3JnLmlldGYvaHR0cF9oZWFkZXIvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsibmFtZSI6Ikhvc3QiLCJ2YWx1ZSI6InNwLnRlZC5jb20ifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy5pZXRmL2h0dHBfaGVhZGVyL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Im5hbWUiOiJPcmlnaW4iLCJ2YWx1ZSI6Imh0dHBzOi8vZnJlc2gtY293LTM3LmxvY2EubHQifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy5pZXRmL2h0dHBfaGVhZGVyL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Im5hbWUiOiJSZWZlcmVyIiwidmFsdWUiOiJodHRwczovL2ZyZXNoLWNvdy0zNy5sb2NhLmx0LyJ9fSx7InNjaGVtYSI6ImlnbHU6b3JnLmlldGYvaHR0cF9oZWFkZXIvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsibmFtZSI6IlVzZXItQWdlbnQiLCJ2YWx1ZSI6Ik1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwXzE1XzcpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS85Mi4wLjQ1MTUuMTA3IFNhZmFyaS81MzcuMzYifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy5pZXRmL2h0dHBfaGVhZGVyL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Im5hbWUiOiJYLUZvcndhcmRlZC1Gb3IiLCJ2YWx1ZSI6Ijc1LjgwLjExMC4xODYifX0seyJzY2hlbWEiOiJpZ2x1OmNvbS5kYmlwL2xvY2F0aW9uL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7ImNpdHkiOnsiZ2VvbmFtZV9pZCI6NTM0MjM1MywibmFtZXMiOnsiZW4iOiJEZWwgTWFyIiwiZmEiOiLYr9mEINmF2KfYsdiMINqp2KfZhNuM2YHYsdmG24zYpyIsImphIjoi44OH44Or44O744Oe44O8IiwiemgtQ04iOiLlvrflsJTpqawifX0sImNvbnRpbmVudCI6eyJjb2RlIjoiTkEiLCJnZW9uYW1lX2lkIjo2MjU1MTQ5LCJuYW1lcyI6eyJkZSI6Ik5vcmRhbWVyaWthIiwiZW4iOiJOb3J0aCBBbWVyaWNhIiwiZXMiOiJOb3J0ZWFtw6lyaWNhIiwiZmEiOiIg2KfZhdix24zaqdin24wg2LTZhdin2YTbjCIsImZyIjoiQW3DqXJpcXVlIER1IE5vcmQiLCJqYSI6IuWMl+OCouODoeODquOCq+Wkp+mZuCIsImtvIjoi67aB7JWE66mU66as7Lm0IiwicHQtQlIiOiJBbcOpcmljYSBEbyBOb3J0ZSIsInJ1Ijoi0KHQtdCy0LXRgNC90LDRjyDQkNC80LXRgNC40LrQsCIsInpoLUNOIjoi5YyX576O5rSyIn19LCJjb3VudHJ5Ijp7Imdlb25hbWVfaWQiOjYyNTIwMDEsImlzX2luX2V1cm9wZWFuX3VuaW9uIjpmYWxzZSwiaXNvX2NvZGUiOiJVUyIsIm5hbWVzIjp7ImRlIjoiVmVyZWluaWd0ZSBTdGFhdGVuIHZvbiBBbWVyaWthIiwiZW4iOiJVbml0ZWQgU3RhdGVzIiwiZXMiOiJFc3RhZG9zIFVuaWRvcyBkZSBBbcOpcmljYSAobG9zKSIsImZhIjoi2KfbjNin2YTYp9iqINmF2KrYrdiv2YfZlCDYp9mF2LHbjNqp2KciLCJmciI6IsOJdGF0cy1VbmlzIiwiamEiOiLjgqLjg6Hjg6rjgqvlkIjooYblm70iLCJrbyI6IuuvuOq1rSIsInB0LUJSIjoiRXN0YWRvcyBVbmlkb3MiLCJydSI6ItCh0KjQkCIsInpoLUNOIjoi576O5Zu9In19LCJsb2NhdGlvbiI6eyJsYXRpdHVkZSI6MzIuOTU5NSwibG9uZ2l0dWRlIjotMTE3LjI2NSwidGltZV96b25lIjoiQW1lcmljYS9Mb3NfQW5nZWxlcyIsIndlYXRoZXJfY29kZSI6IlVTQ0EwMjg4In0sInBvc3RhbCI6eyJjb2RlIjoiOTIwMTQifSwic3ViZGl2aXNpb25zIjpbeyJnZW9uYW1lX2lkIjo1MzMyOTIxLCJpc29fY29kZSI6IkNBIiwibmFtZXMiOnsiZGUiOiJLYWxpZm9ybmllbiIsImVuIjoiQ2FsaWZvcm5pYSIsImVzIjoiQ2FsaWZvcm5pYSIsImZhIjoi2qnYp9mE24zZgdix2YbbjNinIiwiZnIiOiJDYWxpZm9ybmllIiwiamEiOiLjgqvjg6rjg5Xjgqnjg6vjg4vjgqLlt54iLCJrbyI6Iuy6mOumrO2PrOuLiOyVhCDso7wiLCJwdC1CUiI6IkNhbGlmw7NybmlhIiwicnUiOiLQmtCw0LvQuNGE0L7RgNC90LjRjyIsInpoLUNOIjoi5Yqg5Yip56aP5bC85Lqa5beeIn19LHsiZ2VvbmFtZV9pZCI6NTM5MTgzMiwibmFtZXMiOnsiZW4iOiJTYW4gRGllZ28iLCJlcyI6IkNvbmRhZG8gZGUgU2FuIERpZWdvIiwiZmEiOiLYtNmH2LHYs9iq2KfZhiDYs9mGINiv24zar9mI2Iwg2qnYp9mE24zZgdix2YbbjNinIiwiZnIiOiJDb210w6kgZGUgU2FuIERpZWdvIiwiamEiOiLjgrXjg7Pjg4fjgqPjgqjjgrTpg6EiLCJrbyI6IuyDjOuUlOyXkOydtOqzoCDqtbAiLCJwdC1CUiI6IkNvbmRhZG8gZGUgU2FuIERpZWdvIiwicnUiOiLQodCw0L0t0JTQuNC10LPQviIsInpoLUNOIjoi5Zyj6L+t5oiI5Y6/In19XX19LHsic2NoZW1hIjoiaWdsdTpjb20uZGJpcC9pc3AvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsidHJhaXRzIjp7ImF1dG9ub21vdXNfc3lzdGVtX251bWJlciI6MjAwMDEsImF1dG9ub21vdXNfc3lzdGVtX29yZ2FuaXphdGlvbiI6IkNoYXJ0ZXIgQ29tbXVuaWNhdGlvbnMgSW5jIiwiY29ubmVjdGlvbl90eXBlIjoiQ29ycG9yYXRlIiwiaXNwIjoiQ2hhcnRlciBDb21tdW5pY2F0aW9ucyIsIm9yZ2FuaXphdGlvbiI6IlNwZWN0cnVtIn19fV19CWJhZTk4ZWZjLTljNDEtNDQ4Zi1hZDk4LTQ3NjQyZjc5ODMzNgkyMDIxLTA3LTI3IDIxOjI1OjIzLjE5Ngljb20uZ29vZ2xlLmFuYWx5dGljcwlldmVudAlqc29uc2NoZW1hCTEtMC0wCTI2Yzc0MTljOTE1Yjc2NTY1YmE4MzllMWExY2IyNTA3CQ=="
	tsv, err := DecodeBase64Payload(data)
	if err != nil {
		fmt.Println(err)
	}
	// Using Snowplow Analytics GOlang SDK to parse the event
	parsed, err := analytics.ParseEvent(tsv) // Where event is a valid TSV string Snowplow event.
	if err != nil {
		fmt.Println(err)
	}
	// Using Snowplow GOlang SDK GetSubsetMap of derived contexts, the payload
	// that we are interested in
	m_a_p, err = parsed.GetSubsetMap("derived_contexts")
	if err != nil {
		fmt.Println(err)
	}
}

// original example
func TestGetDerivedContextMap(t *testing.T) {
	t.Parallel()
	ctxName := "contexts_nl_basjes_yauaa_context_1"
	derived_context := m_a_p[ctxName]
	output := GetDerivedContextMap(ctxName, derived_context)
	value := output[ctxName]["agentClass"]
	assert.Equal(t, "Browser", value)
}

func TestGetDerivedContextMapV2(t *testing.T) {
	t.Parallel()
	t.Run("contexts_nl_basjes_yauaa_context_1 agentClass", func(t *testing.T) {
		ctxName := "contexts_nl_basjes_yauaa_context_1"
		derived_context := m_a_p[ctxName]
		output := GetDerivedContextMap2(ctxName, derived_context)
		value := output[ctxName].Get("agentClass").String()
		assert.Equal(t, "Browser", value)
	})
	t.Run("contexts_com_dbip_isp_1 traits/connection_type", func(t *testing.T) {
		ctxName := "contexts_com_dbip_isp_1"
		derived_context := m_a_p[ctxName]
		output := GetDerivedContextMap2(ctxName, derived_context)
		value := output[ctxName].Get("traits").Get("connection_type").String()
		assert.Equal(t, "Corporate", value)
	})
	t.Run("contexts_com_dbip_isp_1 traits/autonomous_system_number", func(t *testing.T) {
		ctxName := "contexts_com_dbip_isp_1"
		derived_context := m_a_p[ctxName]
		output := GetDerivedContextMap2(ctxName, derived_context)
		value := output[ctxName].Get("traits").Get("autonomous_system_number").Float64()
		assert.Equal(t, float64(20001), value)
	})
	t.Run("contexts_org_ietf_http_header_1[0] name", func(t *testing.T) {
		ctxName := "contexts_org_ietf_http_header_1"
		derived_context := m_a_p[ctxName]
		output := GetDerivedContextMap2(ctxName, derived_context)
		value := output[ctxName].At(0).Get("name").String()
		assert.Equal(t, "Host", value)
	})
	t.Run("contexts_org_ietf_http_header_1[0] value", func(t *testing.T) {
		ctxName := "contexts_org_ietf_http_header_1"
		derived_context := m_a_p[ctxName]
		output := GetDerivedContextMap2(ctxName, derived_context)
		value := output[ctxName].At(0).Get("value").String()
		assert.Equal(t, "sp.ted.com", value)
	})
	t.Run("contexts_org_ietf_http_header_1[3] value", func(t *testing.T) {
		ctxName := "contexts_org_ietf_http_header_1"
		derived_context := m_a_p[ctxName]
		output := GetDerivedContextMap2(ctxName, derived_context)
		value := output[ctxName].At(3).Get("value").String()
		assert.Equal(t, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36", value)
	})
}

func BenchmarkGetDerivedContextMap(b *testing.B) {
	// not really a fair comparison, v1 "parses" the complete derived context, while v2 only access to the requested
	// "path".
	b.Run("contexts_nl_basjes_yauaa_context_1 v1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctxName := "contexts_nl_basjes_yauaa_context_1"
			derived_context := m_a_p[ctxName]
			output := GetDerivedContextMap(ctxName, derived_context)
			_ = output[ctxName]["agentClass"]
		}
	})
	b.Run("contexts_nl_basjes_yauaa_context_1 v2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctxName := "contexts_nl_basjes_yauaa_context_1"
			derived_context := m_a_p[ctxName]
			output := GetDerivedContextMap2(ctxName, derived_context)
			_ = output[ctxName].Get("agentClass")
		}
	})
	b.Run("contexts_com_dbip_isp_1 v2 traits/connection_type", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctxName := "contexts_com_dbip_isp_1"
			derived_context := m_a_p[ctxName]
			output := GetDerivedContextMap2(ctxName, derived_context)
			_ = output[ctxName].Get("traits").Get("connection_type").String()
		}
	})
}

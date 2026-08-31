export default function GuaranteeTable() {
  return (
    <table>
      <thead><tr><th>Boundary</th><th>Guarantee</th></tr></thead>
      <tbody>
        <tr><td>Admission</td><td>An input is acknowledged only after its durable append commits.</td></tr>
        <tr><td>Execution</td><td>An invocation can repeat after failure; only a fenced, committed completion changes state.</td></tr>
        <tr><td>Completion</td><td>State, output, lease removal, and the next mailbox dispatch commit atomically.</td></tr>
        <tr><td>Recovery</td><td>A published checkpoint plus every later committed transition reconstructs state.</td></tr>
        <tr><td>External effects</td><td>The transactional outbox is at least once; sinks deduplicate by message identifier.</td></tr>
      </tbody>
    </table>
  );
}

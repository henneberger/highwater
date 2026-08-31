import Link from '@docusaurus/Link';
import styles from './styles.module.css';

export default function DocsLanding({ children }) {
  return (
    <main className={styles.wrapper}>
      <div className={styles.eyebrow}>Highwater Docs</div>
      <section className={styles.hero}>
        <div className={styles.copy}>
          <h1>Write streaming applications that never lose their place</h1>
          <p className={styles.lead}>
            Highwater turns Python functions into durable, elastic stream processors.
            State, event-time progress, retries, and scaling are part of the platform.
          </p>
          <div className={styles.actions}>
            <Link className="button button--primary button--lg" to="/quickstart">Quickstart</Link>
            <Link className="button button--secondary button--lg" to="/evaluate/durable-streaming">Why Highwater</Link>
          </div>
        </div>
        <pre className={styles.code} aria-label="Durable process example"><code>{`@process.defn(key="account_id")
@dataclass
class Balance:
    total: int = 0

    @process.event
    async def apply(self, event: Deposit):
        self.total += event.amount`}</code></pre>
      </section>
      {children}
    </main>
  );
}

import Link from '@docusaurus/Link';
import styles from './styles.module.css';

export default function FeatureGrid({ items }) {
  return (
    <section className={styles.grid}>
      {items.map((item) => (
        <Link className={styles.card} to={item.href} key={item.title}>
          <span className={styles.kicker}>{item.kicker}</span>
          <h2>{item.title}</h2>
          <p>{item.children}</p>
          <span className={styles.arrow}>Read more →</span>
        </Link>
      ))}
    </section>
  );
}

import type {ReactNode} from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';


export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} - Multi-paradigm Database Engine`}
      description="MonoDB is a modern, multi-paradigm database engine written in Rust.">
    </Layout>
  );
}
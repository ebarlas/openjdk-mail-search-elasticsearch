## OpenJDK Mail Search (Elasticsearch)

![Duke Mascot](duke.png)

Elasticsearch-backed variant of [OpenJDK Mail Search](https://github.com/ebarlas/openjdk-mail-search).

Replaces the DynamoDB backend with Elasticsearch while keeping the same API and website.

https://openjdk.barlasgarden.com

## Architecture

```
  Users
    │
    ▼
┌───────┐              ┌──────────────────────────┐
│  WAF  │              │  Lambda Scheduled Sync   │
└───┬───┘              └─────────────┬────────────┘
    │                                │
    ▼                                │ bulk index
┌────────────┐                       │
│ CloudFront │                       │
└─────┬──────┘                       │
      │ /api/*                       │
      ▼                              │
┌─────────────────────┐              │
│ Lambda API Server   │              │
└──────────┬──────────┘              │
           │ queries                 │
           ▼                         ▼
     ┌─── EC2 ──────────────────────────┐
     │  ┌─── ECS ────────────────────┐  │
     │  │      Elasticsearch         │  │
     │  └────────────────────────────┘  │
     └───────────────┬──────────────────┘
                     │ attached
               ┌─────┴──────┐
               │ EBS Volume │
               └────────────┘
```

WAF and CloudFront handle incoming traffic. CloudFront routes `/api/*` requests to the Lambda API Server, which queries Elasticsearch. The Lambda Scheduled Sync periodically downloads mbox archives from `mail.openjdk.org` and bulk-indexes them into Elasticsearch. Elasticsearch runs as a single-node ECS task on an EC2 instance, with data persisted on an attached EBS volume.

## Project

* `src/sync.py` - CLI/Lambda for seeding and syncing mailing list records into Elasticsearch
* `src/server.py` - AWS Lambda API server backed by Elasticsearch queries
* `src/mbox.py` - mbox parsing utilities
* `site/index.html` - static website with mailing list search interface

## Deployment

The site runs on the same AWS stack as the original (S3, CloudFront, Lambda) with Elasticsearch replacing DynamoDB. See `aws/` for deployment scripts and task definitions.

## Environment

* `ES_URL` - Elasticsearch endpoint (e.g. `https://elastic:pass@host:9200`)
* `INDEX_NAME` - index name (default: `openjdk-mail`)

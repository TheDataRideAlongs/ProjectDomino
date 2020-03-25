# Project Domino

## Scaling COVID public behavior change and anti-misinformation

See also the [private repository](https://github.com/graphistry/ProjectDomino-internal)

**One of the most important steps in stopping the COVID-19 pandemic is influencing mass behavior change for citizens to take appropriate, swift action on mitigating infection and human-to-human contact.** Government officials at all levels have advocated misinformed practices such as dining out or participating in outdoor gatherings that have contributed to amplifying the curve rather than flattening it. At time of writing, the result of poor crisis emergency risk communication has led to over 14,000 US citizens testing positive, 2-20X more are likely untested, and over 200 deaths. The need to influence appropriate behavior and mitigation actions are extreme: The US has shot up from untouched to become the 6th most infected nation. 

**Project Domino accelerates research on developing capabilities for information hygiene at the mass scale necessary for the current national disaster and for future ones.** We develop and enable the use of 3 key data capabilities for modern social discourse: 
* Identifying at-risk behavior groups and viable local behavior change influencers
* Detecting misinformation campaigns
* Automating high-precision interventions

## The interventions

We are working with ethics groups to identify safe interventions along the following lines:

* **Help top social platforms harden themselves**: Trust and safety teams at top social networks need to be able to warn users about misinformation, de-trend it, and potentially take it down before it has served its purpose. The status quo is handling incidents months after the fact. We will provide real-time alert feeds and scoring APIs to help take action during the critical minutes before misinformation gains significant reach.

* **Enable top analysts to investigate coordinated activity**: A minority of groups cause the bulk of the misinformation that gets shared. We are building a high-scale analyst environment featuring technology such as GPU-accelerated visual graph analytics and high-memory notebook servers.

* **Help leaders clean up their community**: Identify and invite community leaders of at-risk groups to use our tools to detect trending misinformation and sift it out from their regular community content.

* **Alert individuals as they are being manipulated**: For manipulated conversations where we have clear intelligence, we are exploring an alert bot that will post the misinformation report directly on the thread, or enable community participants or project partners to do so.

* **Enable other platforms**: We expect a growing number of initiatives to benefit from our intelligence and automation capabilities.

## The technologies

* Twitter firehose monitor
* Data integration pipeline for sources of known scams, fraud, lies, bots, propaganda, extremism, and other misinformation sources
* Misinformation knowledge graph connecting accounts, posts, reports, and models
* Automated GPU / graph / machine learning pipeline
* Automated alerting & reporting pipeline
* Interactive visual analytics environment for data scientists and analysts: GPU, graph, notebooks, ML, ...

## How to help

We are actively seeking several forms of support:

* **Volunteers**: Most immediate priority is on data engineering and advisors on marketing/public health
  * Data engineers: streaming (Kafka, Nifi, ...), graph (ex: Neo4j), GPU (RAPIDS), ML (NLP libs), and databases
  * Analysts: OSINT, threat intel, campaign tracking, ...
  * Data scientists: especially around graph, misinformation, neural networks, NLP, with backgrounds such as security, fraud,  misinformation, marketing
  * Developers & designers: web, automation
  * Marketing
  * Public health and communications: Especially around intervention design

* **APIs and Data**: 
  * Scoring: Anything to help classify Twitter content for being artificial, misinformation, etc.
  * Twitter and misinformation threat feeds: Known-bad actors and content

* **Software Licenses**:
  * **SaaS preferred**
  * Project management
  * Analytics
  * Marketing automation

* **Hardware**: Anything you can provide along the lines of:
  * 1 x Database server (CPU): 32+ cores, Ubuntu 18, 64GB+ RAM, ideally backups
  * 1 x Primary analytics server (GPU) - 32+ CPU cores, 128GB+ CPU RAM, 2-8 GPUs, 64GB+ disk, 2-10TB attached SSD
    * GPUs: Nvidia Pascal or later, 12GB minimum, with 32GB **strongly** preferred (Ex: 32GB Volta)
  * 2 x Secondary / developer servers (GPU) - 8+ CPU cores, 64GB+ CPU RAM, 2-4 GPUs, 64GB+ disk, 1TB attached SSD
    * GPUs: Nvidia Pascal or later, 12GB minimum
  * 3 x Analyst stations (GPU) - 8+ CPU cores, 64GB+ CPU RAM, 1 TB attached SSD, 1-2 GPUs
    * GPUs: Nvidia Pascal or later, 12GB minimum, with 32GB **strongly** preferred (Ex: 32GB Volta)
  * For each, we can work together to setup Ubuntu with remote SSH admin, Nvidia drivers, & (Nvidia-enabled) Docker

* **Sponsors**: Near-term funding until the project finds a more sustainable path is welcome!

## Contact

Please contact [Leo Meyerovich, CEO @ Graphistry](https://www.linkedin.com/in/leo-meyerovich-09649219) and [Sean Griffin, CEO @ DisasterTech](https://www.linkedin.com/in/seanmichaelgriffin/) for support and information

Slack channel: #COVID on [https://thedataridealongs.slack.com/](https://thedataridealongs.slack.com/)

<template>
  <main class="page-main">
    <section class="hero-card">
      <div class="hero-top">
        <div>
          <span class="hero-eyebrow">系统首页</span>
          <h1 class="hero-title">面向多源遥感 ARD 的一体化运行总览门户</h1>
          <p class="hero-desc">
            系统依据需求分析书中的总体建设目标，围绕“接入-生产-质控-存储索引-服务-统计审计”闭环，统一承接 ARD 数据接入登记、格网化剖分生产、自动化质量控制、剖分成果管理、检索下载、在线计算、资源调度与运维监测等核心能力，支撑 PB 级遥感数据持续生产与高并发服务场景下的稳定运行。
          </p>
          <div class="hero-actions">
            <a class="btn btn-primary" href="#overview">查看系统总览</a>
            <a class="btn btn-secondary" href="#workflow">查看业务闭环</a>
          </div>
        </div>
        <aside class="hero-side">
          <div class="hero-side-title">系统定位</div>
          <div class="hero-side-list">
            <div class="hero-side-item">
              <div class="hero-side-label">建设角色</div>
              <div>国家对地观测科学数据中心体系化能力提升项目关键支撑系统</div>
            </div>
            <div class="hero-side-item">
              <div class="hero-side-label">空间基准</div>
              <div>以全球离散格网为统一基准，建立分层格网与时空编码体系</div>
            </div>
            <div class="hero-side-item">
              <div class="hero-side-label">服务对象</div>
              <div>覆盖管理员、科学家团队、高级用户与普通用户四类角色</div>
            </div>
            <div class="hero-side-item">
              <div class="hero-side-label">运行目标</div>
              <div>支撑大规模持续生产、高并发检索与弹性在线计算的一体化运行</div>
            </div>
          </div>
        </aside>
      </div>
      <div class="hero-metrics">
        <div class="metric-box">
          <div class="metric-value">9PB</div>
          <div class="metric-label">总存储资源规模</div>
          <div class="metric-note">其中 8.5PB 为在线存储底座</div>
        </div>
        <div class="metric-box">
          <div class="metric-value">1000+</div>
          <div class="metric-label">并发检索用户能力</div>
          <div class="metric-note">面向高并发检索与地图浏览场景</div>
        </div>
        <div class="metric-box">
          <div class="metric-value">100+</div>
          <div class="metric-label">并发在线计算能力</div>
          <div class="metric-note">基于容器化与弹性伸缩机制设计</div>
        </div>
        <div class="metric-box">
          <div class="metric-value">24h</div>
          <div class="metric-label">持续运行保障</div>
          <div class="metric-note">支持不中断运行与故障迁移恢复</div>
        </div>
      </div>
    </section>

    <section id="overview" class="section-card">
      <div class="section-head">
        <div class="section-head-main">
          <div class="section-tag">Platform Overview</div>
          <div class="section-title">平台简介</div>
        </div>
      </div>
      <div class="parallel-showcase">
        <div class="parallel-grid">
          <article class="parallel-card">
            <div class="parallel-head">
              <span class="parallel-icon" aria-hidden="true">
                <svg viewBox="0 0 24 24"><path d="M8 3h8l-2.5 4.5L16 12l-4 4-4-4 2.5-4.5z"></path></svg>
              </span>
              <div class="parallel-title">系统介绍</div>
            </div>
            <p class="parallel-text">
              分析就绪数据剖分管理系统面向多源遥感分析就绪数据及相关信息产品，提供从数据接入登记、任务编排、格网化剖分生产，到自动化质控、入库与版本管理的闭环能力，并通过统一门户承接运维监控、业务统计与服务展示。
            </p>
            <div class="parallel-metric"><strong>6</strong><span>大业务域协同</span></div>
            <div class="parallel-actions">
              <a class="parallel-btn primary" href="#workflow">系统流程</a>
              <a class="parallel-btn secondary" href="#">总体架构</a>
            </div>
          </article>

          <article class="parallel-card">
            <div class="parallel-head">
              <span class="parallel-icon" aria-hidden="true">
                <svg viewBox="0 0 24 24"><path d="M4 18V6"></path><path d="M10 18V10"></path><path d="M16 18V8"></path><path d="M22 18V4"></path></svg>
              </span>
              <div class="parallel-title">系统运行情况</div>
            </div>
            <p class="parallel-text">
              系统围绕“接入-生产-质控-存储索引-服务-统计审计”闭环稳定运行，支持任务积压监测、故障告警、负载迁移与 24 小时连续服务，兼顾自动化调度与人工干预相结合的运维方式。
            </p>
            <div class="parallel-metric"><strong>24h</strong><span>持续运行保障</span></div>
            <div class="parallel-actions">
              <a class="parallel-btn primary" href="#">运行态势</a>
              <a class="parallel-btn secondary" href="#">监测统计</a>
            </div>
          </article>

          <article class="parallel-card">
            <div class="parallel-head">
              <span class="parallel-icon" aria-hidden="true">
                <svg viewBox="0 0 24 24"><path d="M12 3l3 5 5 .5-3.5 3 1 5-5.5-2.8L7 16.5l1-5L4.5 8.5 9.5 8z"></path></svg>
              </span>
              <div class="parallel-title">系统资源情况</div>
            </div>
            <p class="parallel-text">
              系统配置总计 9PB 存储资源，其中 8.5PB 为在线存储底座；通过负载均衡、热点缓存和水平扩展设计，满足不少于 1000 个并发检索用户和不少于 100 个并发在线计算用户的能力目标。
            </p>
            <div class="parallel-metric"><strong>9PB</strong><span>总存储资源规模</span></div>
            <div class="parallel-actions">
              <a class="parallel-btn primary" href="#">资源概览</a>
              <a class="parallel-btn secondary" href="#">能力指标</a>
            </div>
          </article>

          <article class="parallel-card">
            <div class="parallel-head">
              <span class="parallel-icon" aria-hidden="true">
                <svg viewBox="0 0 24 24"><path d="M4 7h16"></path><path d="M7 12h10"></path><path d="M9 17h6"></path><rect x="3" y="4" width="18" height="16" rx="2"></rect></svg>
              </span>
              <div class="parallel-title">数据情况</div>
            </div>
            <p class="parallel-text">
              系统兼顾一致性光学遥感数据、雷达遥感数据、碳卫星 L2 级以上产品数据及遥感信息产品处理要求，支持平面、经纬度、六边形三种格网体系，并按阶段推进 3.5PB 数据接入与高质量入库。
            </p>
            <div class="parallel-metric"><strong>4类</strong><span>主要数据对象</span></div>
            <div class="parallel-actions">
              <a class="parallel-btn primary" href="#">查看数据</a>
              <a class="parallel-btn secondary" href="#">数据类型</a>
            </div>
          </article>
        </div>
      </div>
    </section>

    <section id="workflow" class="section-card">
      <div class="section-head">
        <div class="section-head-main">
          <div class="section-tag">Workflow</div>
          <div class="section-title">业务闭环</div>
          <div class="section-subtitle">首页展示系统总体运行链路，帮助用户快速理解平台工作方式</div>
        </div>
      </div>
      <div class="steps-grid">
        <article class="step-card">
          <div class="step-index">01</div>
          <h3 class="step-title">数据接入</h3>
          <p class="step-text">对接业务系统对象清单，完成路径校验、元数据提取与接入登记。</p>
        </article>
        <article class="step-card">
          <div class="step-index">02</div>
          <h3 class="step-title">任务编排</h3>
          <p class="step-text">按数据类型、处理策略和优先级生成批次化剖分与服务任务。</p>
        </article>
        <article class="step-card">
          <div class="step-index">03</div>
          <h3 class="step-title">格网剖分</h3>
          <p class="step-text">在统一格网体系下完成空间切片、多维重构和标准化处理。</p>
        </article>
        <article class="step-card">
          <div class="step-index">04</div>
          <h3 class="step-title">自动质检</h3>
          <p class="step-text">对物理完整性、元数据合规性和像素值有效性进行并行检测。</p>
        </article>
        <article class="step-card">
          <div class="step-index">05</div>
          <h3 class="step-title">存储索引</h3>
          <p class="step-text">将剖分成果、索引与质检摘要纳入统一存储与检索体系。</p>
        </article>
        <article class="step-card">
          <div class="step-index">06</div>
          <h3 class="step-title">服务统计</h3>
          <p class="step-text">对外提供浏览、检索、下载、在线计算和统计审计能力。</p>
        </article>
      </div>
    </section>

    <div class="footer-note">
      首页内容依据《分析就绪数据剖分管理系统需求分析》整理，当前已按参考图调整为四列并行展示样式，并保留系统总体介绍、运行能力、资源规模、数据概况等总览信息。
    </div>
  </main>
</template>

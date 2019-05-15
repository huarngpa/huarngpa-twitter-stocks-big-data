<template>
  <div>
    <section class="hero is-primary">
      <div class="hero-body">
        <div class="container has-text-centered">
          <h2 class="title">Twitter and Stocks Big Data Project</h2>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="container">
        <div class="card">
          <div class="card-content">
            <p class="title">System Data Requests</p>
            <p class="subtitle">Make or check data requests to the system.</p>
          </div>
          <div class="card-footer">
            <router-link to="/requests" class="card-footer-item">Create Request</router-link>
          </div>
        </div>
        <br/>
        <div class="card">
          <div class="card-content">
            <p class="title">Twitter Data</p>
            <p class="subtitle">Analyze weekly statistics in Twitter data.</p>
          </div>
          <div class="card-footer">
            <router-link to="/twitter-weekly" class="card-footer-item">Check Statistics</router-link>
          </div>
        </div>
        <br/>
        <div class="card">
          <div class="card-content">
            <p class="title">Stock Market Data</p>
            <p class="subtitle">Analyze weekly statistics in the stock market.</p>
          </div>
          <div class="card-footer">
            <router-link to="/stock-weekly" class="card-footer-item">Check Statistics</router-link>
          </div>
        </div>
      </div>
    </section>
  </div>
</template>

<script>
</script>

<style scoped>
</style>

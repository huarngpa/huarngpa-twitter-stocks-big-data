<template>
  <div>
    <section class="hero is-primary">
      <div class="hero-body">
        <div class="container has-text-centered">
          <h2 class="title">System Requests</h2>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="container">
        <div class="card">
          <div class="card-content">
            <p class="title">Twitter User Request</p>
            <p class="subtitle">Request the system to compile statistics and trends on a Twitter user.</p>
          </div>
          <div class="card-footer">
            <router-link to="requests/twitter" class="card-footer-item">Make Request</router-link>
          </div>
        </div>
        <br/>
        <div class="card">
          <div class="card-content">
            <p class="title">Stock Ticker Requests</p>
            <p class="subtitle">Request the system to compile statistics and trends on a company that is traded on an exchange.</p>
          </div>
          <div class="card-footer">
            <router-link to="requests/stock" class="card-footer-item">Make Request</router-link>
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

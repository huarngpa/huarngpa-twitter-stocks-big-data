<template>
  <div>
    <section class="hero is-primary">
      <div class="hero-body">
        <div class="container has-text-centered">
          <h2 class="title">Stock Weekly Statistics</h2>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="container">
        <table class="table is-fullwidth is-bordered is-striped is-hoverable">
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Change</th>
              <th>Open</th>
              <th>Close</th>
              <th>Low</th>
              <th>High</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="ticker in stockWeekly" v-bind:key="ticker.ticker">
              <td>{{ ticker.ticker }}</td>
              <td>{{ ticker['weekly:avg_day_change'] }}</td>
              <td>{{ ticker['weekly:avg_day_open'] }}</td>
              <td>{{ ticker['weekly:avg_day_close'] }}</td>
              <td>{{ ticker['weekly:min_day_low'] }}</td>
              <td>{{ ticker['weekly:max_day_high'] }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>

<script>
import { mapState } from 'vuex'

export default {
  computed: mapState({
    stockWeekly: state => state.stockWeekly
  }),
  beforeMount () {
    this.$store.dispatch('loadStockWeekly')
  }
}
</script>

<style scoped>
</style>

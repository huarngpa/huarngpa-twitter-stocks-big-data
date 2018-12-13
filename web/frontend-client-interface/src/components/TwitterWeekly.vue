<template>
  <div>
    <section class="hero is-primary">
      <div class="hero-body">
        <div class="container has-text-centered">
          <h2 class="title">Twitter Weekly Statistics</h2>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="container">
        <table class="table is-fullwidth is-bordered is-striped is-hoverable">
          <thead>
            <tr>
              <th>ID</th>
              <th>Username</th>
              <th>Tweets</th>
              <th>Favorited</th>
              <th>Retweets</th>
              <th>Sentiment</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="twitterUser in twitterWeekly" v-bind:key="twitterUser.user_id">
              <td>{{ twitterUser['user_id'] }}</td>
              <td>{{ twitterUser['username'] }}</td>
              <td>{{ twitterUser['weekly:count_tweets'] }}</td>
              <td>{{ twitterUser['weekly:sum_favorited'] }}</td>
              <td>{{ twitterUser['weekly:sum_retweets'] }}</td>
              <td>{{ twitterUser['weekly:sum_sentiment'] }}</td>
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
    twitterWeekly: state => state.twitterWeekly
  }),
  beforeMount () {
    this.$store.dispatch('loadTwitterWeekly')
  }
}
</script>

<style scoped>
</style>

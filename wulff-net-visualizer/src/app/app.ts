import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { WulffNet } from './wulff-net';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule, WulffNet],
  templateUrl: './app.html'
})
export class AppComponent {
  azimuth = 0;
  polar = 0;
  isSphereMode = false;
  uvw = { u: 0, v: 0, w: 1 };

  // Кнопка упрощения
  simplifyCurrentView() {
    const phi = -(this.azimuth) * (Math.PI / 180);
    const theta = (this.polar) * (Math.PI / 180);

    const u_raw = Math.sin(theta) * Math.cos(phi);
    const v_raw = -(Math.sin(theta) * Math.sin(phi));
    const w_raw = Math.cos(theta);

    this.uvw = this.calculateSimpleIndices(u_raw, v_raw, w_raw);
    this.onManualInputChange();
  }

  // При ручном вводе индексов Миллера
  onManualInputChange() {
    const { u, v, w } = this.uvw;
    const length = Math.sqrt(u*u + v*v + w*w);
    if (length > 0) {
      this.azimuth = Math.round(((-Math.atan2(-v, u) * 180 / Math.PI) + 360) % 360);
      this.polar = Math.round(Math.acos(w / length) * (180 / Math.PI));
    }
  }

  handleUvwChange(newUvw: { u: number, v: number, w: number }) {
    this.uvw = newUvw;
  }

  private calculateSimpleIndices(u: number, v: number, w: number) {
    const maxVal = Math.max(Math.abs(u), Math.abs(v), Math.abs(w));
    if (maxVal === 0) return { u: 0, v: 0, w: 1 };

    const factor = 10; 
    const U = Math.round((u / maxVal) * factor);
    const V = Math.round((v / maxVal) * factor);
    const W = Math.round((w / maxVal) * factor);

    const gcd = (a: number, b: number): number => b === 0 ? Math.abs(a) : gcd(b, a % b);
    const common = gcd(U, gcd(V, W)) || 1;

    return { u: U / common, v: V / common, w: W / common };
  }
}